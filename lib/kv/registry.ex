defmodule KV.Registry do
	use GenServer

	def start_link(opts) do
		GenServer.start_link(__MODULE__, :ok, opts)
	end

	@doc """
  Looks up the bucket pid for `name` stored in `server`.

  Returns `{:ok, pid}` if the bucket exists, `:error` otherwise.
	"""
	def lookup(server, name) do
		GenServer.call(server, {:lookup, name})
	end

	def create(server, name) do
    GenServer.call(server, {:create, name})
	end

	def stop(server) do
		GenServer.stop(server)
	end
	
	## Server Callbacks

	def init(:ok) do
		names = %{}
		refs = %{}
    {:ok, {names, refs}}
  end

  def handle_call({:lookup, name}, _from, {names, _} = state) do
    {:reply, Map.fetch(names, name), state}
  end

	def handle_call({:create, name}, _from, {names, _} = state) do
		case Map.has_key?(names, name) do
			true	-> {:reply, {:ok, name}, state}
			false	-> create_bucket(name, state)
		end
	end

	defp create_bucket(name, {names, refs}) do
		{:ok, pid} = DynamicSupervisor.start_child(KV.BucketSupervisor, KV.Bucket)

		ref = Process.monitor(pid)
		refs = Map.put(refs, ref, name)
		names = Map.put(names, name, pid)

		{:reply, {:ok, name}, {names, refs}}
	end
	
	@doc """
	Handle :DOWN message when the bucket is stoped or exits
	"""
	def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, refs}) do
		{name, refs} = Map.pop(refs, ref)
		names = Map.delete(names, name)
		{:noreply, {names, refs}}
	end

	@doc """
	Handle unknown messages
	"""
	def handle_info(_msg, state) do
		{:noreply, state}
	end
end
