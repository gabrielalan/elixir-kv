require Logger

defmodule KVServer do
  def accept(port) do
    # The options below mean:
    #
    # 1. `:binary` - receives data as binaries (instead of lists)
    # 2. `packet: :line` - receives data line by line
    # 3. `active: false` - blocks on `:gen_tcp.recv/2` until data is available
    # 4. `reuseaddr: true` - allows us to reuse the address if the listener crashes
    #
    {:ok, socket} = :gen_tcp.listen(port, [:binary, packet: :line, active: false, reuseaddr: true])
    Logger.info "Accepting connections on port #{port}"
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    {:ok, pid} = Task.Supervisor.start_child(KVServer.TaskSupervisor, fn -> serve(client) end)
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end

  defp serve(socket) do
    socket
      |> read_transform_line()
      |> write_line(socket)

    serve(socket)
  end

  defp read_transform_line(socket) do
    with {:ok, data} <- read_line(socket),
      {:ok, command} <- KVServer.Command.parse(data),
      do: KVServer.Command.run(command)
  end

  defp read_line(socket) do
    :gen_tcp.recv(socket, 0)
  end

  defp write_line({:ok, text}, socket) do
    :gen_tcp.send(socket, text)
  end

  defp write_line({:error, :unknown_command}, socket) do
    # Known error. Write to the client.
    :gen_tcp.send(socket, "UNKNOWN COMMAND\r\n")
  end

  defp write_line({:error, :closed}, _socket) do
    # The connection was closed, exit politely.
    exit(:shutdown)
  end

  defp write_line({:error, :not_found}, socket) do
    :gen_tcp.send(socket, "NOT FOUND\r\n")
  end
  
  defp write_line({:error, error}, socket) do
    # Unknown error. Write to the client and exit.
    :gen_tcp.send(socket, "ERROR\r\n")
    exit(error)
  end
end
