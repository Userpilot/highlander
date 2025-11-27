defmodule Highlander do
  @moduledoc """
  Highlander allows you to run a single globally unique process in a cluster.

  Highlander uses erlang's `:global` module to ensure uniqueness, and uses `child_spec.id` as the uniqueness key.

  Highlander will start its child process just once in a cluster. The first Highlander process will start its child, all other Highlander processes will monitor the first process and attempt to take over when it goes down.

  _Note: You can also use Highlander to start a globally unique supervision tree._

  ## Usage
  Simply wrap a child process with `{Highlander, child}`.

  Before:

  ```
  children = [
    child_spec
  ]

  Supervisor.init(children, strategy: :one_for_one)
  ```

  After:

  ```
  children = [
    {Highlander, child_spec}
  ]

  Supervisor.init(children, strategy: :one_for_one)
  ```
  See the [documentation on Supervisor.child_spec/1](https://hexdocs.pm/elixir/Supervisor.html#module-child_spec-1) for more information.

  ## `child_spec.id` is used to determine global uniqueness

  Ensure that `child_spec.id` has the correct value! Check the debug logs if you are unsure what is being used.

  ## Globally unique supervisors

  You can also have Highlander run a supervisor:

  ```
  children = [
    {Highlander, {MySupervisor, arg}},
  ]
  ```

  ## Handling netsplits

  If there is a netsplit in your cluster, then Highlander will think that the other process has died, and start a new one. When the split heals, `:global` will recognize that there is a naming conflict, and will take action to rectify that. To deal with this, Highlander simply terminates one of the two child processes with reason `:shutdown`.

  To catch this, simply trap exits in your process and add a `terminate/2` callback.

  Note: The `terminate/2` callback will also run when your application is terminating.

  ```
  def init(arg) do
    Process.flag(:trap_exit, true)
    {:ok, initial_state(arg)}
  end

  def terminate(_reason, _state) do
    # this will run when the process receives an exit signal from its parent
  end
  ```
  """

  use GenServer
  require Logger

  def child_spec(child_child_spec) do
    child_child_spec = Supervisor.child_spec(child_child_spec, [])

    Logger.debug("Starting Highlander with #{inspect(child_child_spec.id)} as uniqueness key")

    %{
      id: child_child_spec.id,
      start: {GenServer, :start_link, [__MODULE__, child_child_spec, []]}
    }
  end

  @impl true
  @spec init(%{:id => any(), optional(any()) => any()}) ::
          {:ok,
           %{
             :child_spec => %{:id => any(), optional(any()) => any()},
             optional(:pid) => pid(),
             optional(any()) => any()
           }}
  def init(child_spec) do
    Process.flag(:trap_exit, true)
    {:ok, register(%{child_spec: child_spec})}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, reason}, %{ref: ref} = state) do
    # When a node disconnects, the monitored process sends DOWN with :nodedown reason
    # Retry registration after a short delay to allow cluster to stabilize
    case reason do
      :nodedown ->
        Logger.info(
          "Highlander detected node disconnection for #{inspect(state.child_spec.id)}. Retrying registration..."
        )

        Process.send_after(self(), :retry_register, 1000)
        {:noreply, state}

      _ ->
        # Normal process termination, retry immediately
        {:noreply, register(state)}
    end
  end

  def handle_info({:EXIT, _pid, :name_conflict}, %{pid: pid} = state) do
    :ok = Supervisor.stop(pid, :shutdown)
    {:stop, {:shutdown, :name_conflict}, Map.delete(state, :pid)}
  end

  def handle_info(:retry_register, state) do
    # Retry registration after node disconnections or errors
    {:noreply, register(state)}
  end

  @impl true
  def terminate(reason, %{pid: pid}) do
    :ok = Supervisor.stop(pid, reason)
  end

  def terminate(_, _), do: nil

  defp name(%{child_spec: %{id: global_name}}) do
    {__MODULE__, global_name}
  end

  defp handle_conflict(_name, pid1, pid2) do
    # During node disconnections, :global may call this even for already-registered processes
    # Try to determine which process should keep the name by checking if either is self()
    # and if self() has a child running. If self() is pid1 and we have a child, keep pid1.
    # Otherwise, prefer keeping the process that's already registered (check via whereis_name).
    # For simplicity, we'll keep pid1 (the first one) as default, but this should rarely
    # be called if we prevent re-registration when already registered.

    # If pid1 is self(), try to check if we should keep the name
    if pid1 == self() do
      # We're pid1, check if we should keep the name
      # For now, keep pid1 (self) and exit pid2
      Process.exit(pid2, :name_conflict)
      pid1
    elsif pid2 == self() do
      # We're pid2, but pid1 was registered first, so exit self
      Process.exit(self(), :name_conflict)
      pid1
    else
      # Neither is self (shouldn't happen, but handle it)
      Process.exit(pid2, :name_conflict)
      pid1
    end
  end

  defp register(state) do
    # If we already have a pid, we're already registered and running
    # Don't try to register again
    if Map.has_key?(state, :pid) do
      Logger.debug(
        "Highlander for #{inspect(state.child_spec.id)} already registered and running, skipping registration"
      )

      state
    else
      try do
        case :global.register_name(name(state), self(), &handle_conflict/3) do
          :yes -> start(state)
          :no -> monitor(state)
        end
      rescue
        e ->
          # During node disconnections, :global operations can fail
          # Log the error and retry after a delay
          Logger.warning(
            "Highlander registration failed for #{inspect(state.child_spec.id)}: #{inspect(e)}. Retrying in 1 second..."
          )

          Process.send_after(self(), :retry_register, 1000)
          state
      catch
        :exit, {:nodedown, _node} ->
          # Node disconnected during registration
          Logger.warning(
            "Highlander registration failed for #{inspect(state.child_spec.id)} due to node disconnection. Retrying in 1 second..."
          )

          Process.send_after(self(), :retry_register, 1000)
          state

        :exit, reason ->
          # Other exit reasons - log and retry
          Logger.warning(
            "Highlander registration failed for #{inspect(state.child_spec.id)}: #{inspect(reason)}. Retrying in 1 second..."
          )

          Process.send_after(self(), :retry_register, 1000)
          state
      end
    end
  end

  defp start(state) do
    case Supervisor.start_link([state.child_spec], strategy: :one_for_one) do
      {:ok, pid} ->
        Map.put(state, :pid, pid)

      {:error, reason} ->
        # If supervisor fails to start, log and retry after delay
        Logger.error(
          "Highlander failed to start supervisor for #{inspect(state.child_spec.id)}: #{inspect(reason)}. Retrying in 1 second..."
        )

        Process.send_after(self(), :retry_register, 1000)
        state
    end
  end

  defp monitor(state) do
    try do
      case :global.whereis_name(name(state)) do
        :undefined ->
          register(state)

        pid ->
          ref = Process.monitor(pid)
          %{child_spec: state.child_spec, ref: ref}
      end
    rescue
      e ->
        # During node disconnections, :global operations can fail
        Logger.warning(
          "Highlander whereis_name failed for #{inspect(state.child_spec.id)}: #{inspect(e)}. Retrying in 1 second..."
        )

        Process.send_after(self(), :retry_register, 1000)
        state
    catch
      :exit, {:nodedown, _node} ->
        # Node disconnected during lookup
        Logger.warning(
          "Highlander whereis_name failed for #{inspect(state.child_spec.id)} due to node disconnection. Retrying in 1 second..."
        )

        Process.send_after(self(), :retry_register, 1000)
        state

      :exit, reason ->
        # Other exit reasons - log and retry
        Logger.warning(
          "Highlander whereis_name failed for #{inspect(state.child_spec.id)}: #{inspect(reason)}. Retrying in 1 second..."
        )

        Process.send_after(self(), :retry_register, 1000)
        state
    end
  end
end
