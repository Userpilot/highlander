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
      start: {GenServer, :start_link, [__MODULE__, child_child_spec, []]},
      restart: :permanent
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

  def handle_info(:retry_register, state) do
    # Retry registration after node disconnections or errors
    {:noreply, register(state)}
  end

  def handle_info(:check_registration, state) do
    # Periodic check to ensure we're still registered or re-register if needed
    # This handles the case where all processes unregister during scale-down
    {:noreply, ensure_registered(state)}
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
    # We need to determine which process should keep the name.
    # The process that was registered first (pid1) should keep it.
    # If we're pid2, we should give up and let pid1 keep it.
    # Highlander will then monitor pid1 and take over if it fails.
    #
    # Note: When we return pid1, :global.register_name will return :no for pid2,
    # causing pid2 to go to monitor mode automatically.

    # Always let pid1 (the first registered process) keep the name
    Logger.warning("Highlanderhandle_conflict: #{inspect(pid1)} vs #{inspect(pid2)}")
    pid1
  end

  defp register(state) do
    # If we already have a pid, verify we're still registered
    if Map.has_key?(state, :pid) do
      # Check if we're still the registered owner
      registered_pid = :global.whereis_name(name(state))

      cond do
        registered_pid == self() ->
          # We're still registered, keep current state
          Logger.debug(
            "Highlander for #{inspect(state.child_spec.id)} already registered and running"
          )
          state

        registered_pid == :undefined ->
          # We have a pid but we're not registered - this can happen during scale-down
          # Stop the supervisor and re-register
          Logger.warning(
            "Highlander for #{inspect(state.child_spec.id)} has pid but is not registered. Re-registering..."
          )

          :ok = Supervisor.stop(state.pid, :shutdown)
          state_without_pid = Map.delete(state, :pid)
          register(state_without_pid)

        true ->
          # Someone else is registered, stop our supervisor and monitor them
          Logger.warning(
            "Highlander for #{inspect(state.child_spec.id)} lost registration to #{inspect(registered_pid)}. Stopping supervisor and monitoring..."
          )

          :ok = Supervisor.stop(state.pid, :shutdown)
          state_without_pid = Map.delete(state, :pid)
          monitor(state_without_pid)
      end
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

  defp ensure_registered(state) do
    # Ensure we're in the correct state - either registered and running, or monitoring
    if Map.has_key?(state, :pid) do
      # We think we're registered, verify
      registered_pid = :global.whereis_name(name(state))

      cond do
        registered_pid == self() ->
          # We're registered, schedule next check
          schedule_registration_check()
          state

        registered_pid == :undefined ->
          # Lost registration, re-register
          Logger.warning(
            "Highlander for #{inspect(state.child_spec.id)} lost registration. Re-registering..."
          )

          :ok = Supervisor.stop(state.pid, :shutdown)
          state_without_pid = Map.delete(state, :pid)
          register(state_without_pid)

        true ->
          # Someone else is registered, monitor them
          Logger.warning(
            "Highlander for #{inspect(state.child_spec.id)} found other registered process #{inspect(registered_pid)}. Stopping supervisor and monitoring..."
          )

          :ok = Supervisor.stop(state.pid, :shutdown)
          state_without_pid = Map.delete(state, :pid)
          monitor(state_without_pid)
      end
    else
      # We're monitoring, verify the monitored process still exists
      if Map.has_key?(state, :ref) do
        # We have a ref, check if the process is still alive
        case :global.whereis_name(name(state)) do
          :undefined ->
            # No one is registered, try to register
            register(state)

          pid ->
            # Process is still registered, keep monitoring
            schedule_registration_check()
            state
        end
      else
        # No pid and no ref, try to register
        register(state)
      end
    end
  end

  defp schedule_registration_check do
    # Schedule a periodic check every 5 seconds to ensure we're still registered
    Process.send_after(self(), :check_registration, 5000)
  end

  defp start(state) do
    case Supervisor.start_link([state.child_spec], strategy: :one_for_one) do
      {:ok, pid} ->
        schedule_registration_check()
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
          # No one is registered, try to register immediately
          register(state)

        pid ->
          ref = Process.monitor(pid)
          schedule_registration_check()
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
