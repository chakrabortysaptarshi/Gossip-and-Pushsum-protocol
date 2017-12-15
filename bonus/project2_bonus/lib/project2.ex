defmodule ConvergenceTracker do
  use GenServer

  def add_convergedNode(pid, message) do
    GenServer.cast(pid, {:add_convergedNode, message})
  end

  def get_nonconvergednode(pid, nodeId, topology, numNodes) do
    GenServer.call(pid, {:get_nonconvergednode, nodeId, topology, numNodes}, :infinity)
  end

  def get_convergedNode(pid) do
    GenServer.call(pid, :get_convergedNode, :infinity)
  end

  def getNonConvergedNeighbor(topology, total, nodeId, messages) do
    nonConvergednodeList = Topologies.topology_nonconverged_nodes(topology, total, nodeId)
    nonConvergednodeList = Enum.filter(nonConvergednodeList, fn el -> !Enum.member?(messages, el) end)
    nonConvergednodeLen = Kernel.length(nonConvergednodeList)
    lineortwodtopology = false
    if topology == "line" or topology == "2D" do
      lineortwodtopology = true
    end
    if nonConvergednodeLen == 0 and lineortwodtopology == true do
      # This checks for no convergence for line and 2D topology
      :timer.sleep 1000
      Process.exit(:global.whereis_name(:"vagisha"),:kill)
    end
    if nonConvergednodeLen == 0 do
      getNonConvergedNeighbor(topology, total, nodeId, messages)
    else
      nonconvergedrandomneighbor = :rand.uniform(nonConvergednodeLen)
      Enum.at(nonConvergednodeList, nonconvergedrandomneighbor-1)
    end
  end

  def init(messages) do
    {:ok, messages}
  end

  def handle_call({:get_nonconvergednode, nodeId, topology, total}, _from, messages) do
    newNodeId = getNonConvergedNeighbor(topology, total, nodeId, messages)
    {:reply, newNodeId, messages}
  end

  def handle_cast({:add_convergedNode, new_message}, messages) do
    {:noreply, [new_message | messages]}
  end

  def handle_call(:get_convergedNode, _from, messages) do
    {:reply, messages, messages}
  end

end

defmodule Topologies do
    def get_neighbors(topology, total, nodeId) do
        max = total
        cond do
            topology == "line" ->
                cond do
                    nodeId == 1 -> neighbor = [nodeId+1]
                    nodeId == max -> neighbor = [nodeId-1]
                    true -> neighbor = [nodeId+1, nodeId-1]       
                end

            topology == "full" -> neighbor=Enum.to_list(1..max)

            topology == "2D" or topology == "imp2D" ->
                newNode = :math.sqrt(total) |> round
                neighbor = []
                if rem(nodeId,newNode) == 0 do
                    neighbor = neighbor ++ [nodeId+1]
                end
                if rem(nodeId+1,newNode) do
                    neighbor = neighbor ++ [nodeId-1]
                end
                if nodeId-newNode<0 do
                    neighbor = neighbor ++ [nodeId+newNode]
                end
                if nodeId - (total-newNode) >= 0 do
                    neighbor = neighbor ++ [nodeId-newNode]
                end
                if total > 4 do
                    if rem(nodeId,newNode) != 0 and rem(nodeId+1,newNode) != 0 do
                        neighbor = neighbor ++ [nodeId-1]
                        neighbor = neighbor ++ [nodeId+1]
                    end
                    if nodeId-newNode>0 and nodeId - (total-newNode) <0 do
                        neighbor = neighbor ++ [nodeId+newNode]
                        neighbor = neighbor ++ [nodeId-newNode]
                    end
                    if nodeId == newNode do
                        neighbor = neighbor ++ [nodeId-newNode]
                        neighbor = neighbor ++ [nodeId+newNode]
                    end
                end
                if topology == "imp2D" do
                    rnd = getRandomNodeForimp2D(total, neighbor, nodeId)
                    neighbor = neighbor ++ [rnd]
                end
                neighbor
            
            true -> "Select a valid topology"
        end
    end

    def topology_nonconverged_nodes(topology, total, nodeId) do
        neighbors = get_neighbors(topology, total, nodeId)
        neighbors = Enum.filter(neighbors, fn(x) -> x != nodeId == true end)
        neighbors = Enum.filter(neighbors, fn(x) -> x != 0 == true end)
        neighbors = Enum.filter(neighbors, fn(x) -> x <= total == true end)
        neighbors = Enum.uniq(neighbors)
        neighbors
    end

     def getRandomNodeForimp2D(total, neighbor, nodeId) do
        randomNode = :rand.uniform(total)
        if randomNode == nodeId or Enum.member?(neighbor, randomNode) == true do
            getRandomNodeForimp2D(total, neighbor, nodeId)
        else
            randomNode
        end
    end
end


# this is the module responsible for handling all the gossip related code
defmodule Gossip do
  use GenServer

  def add_gossip(pid, message, nodeId, topo, total) do
    GenServer.cast(pid, {:add_gossip, message, nodeId, topo, total})
  end

  def checkConvergence(total, starttime, topology, isTriggered, bonusparam) do
    convergedList = ConvergenceTracker.get_convergedNode(:global.whereis_name(:"convTracker"))
    convergedTotal = Kernel.length(convergedList)
    if topology == "line" or topology == "2D" do
      convergencethreshold = 0.05
    else 
      convergencethreshold = 0.05
    end

    connectionthreshold = convergencethreshold/2
    if convergedTotal / total >= connectionthreshold and isTriggered == false do
      # randomly snap connection for specific percentage of nodes
      isTriggered = true
      Enum.map(1..bonusparam, fn(_) ->
         bonusnode = :rand.uniform(total)
         ConvergenceTracker.add_convergedNode(:global.whereis_name(:"convTracker"), bonusnode)
      end)
    end

    if(convergedTotal / total >= convergencethreshold) do
      IO.puts "Convergence Time = #{System.system_time(:millisecond) - starttime}"
      Process.exit(self(),:kill)
    end
    checkConvergence(total, starttime, topology, isTriggered, bonusparam)
  end
  

  def init(messages) do
    {:ok, messages}
  end

  # helper function to receieve the gossip message and propagate it to the next node
  def handle_cast({:add_gossip, new_message, nodeId, topology, total}, messages) do
    if messages == 9 do
      ConvergenceTracker.add_convergedNode(:global.whereis_name(:"convTracker"), nodeId)
    end
    nextNodeId = ConvergenceTracker.get_nonconvergednode(:global.whereis_name(:"convTracker"), nodeId, topology, total)
    nextnodeName = String.to_atom("node#{nextNodeId}")
    :timer.sleep 1
    Gossip.add_gossip(:global.whereis_name(nextnodeName), new_message, nextNodeId, topology, total)
    {:noreply, messages+1}
  end

  # creates the actors/ nodes according to the input parameter. every node has an actor associated with it
  def generateActors(totalNodes) do
    if totalNodes > 0 do
      newnodeName = String.to_atom("node#{totalNodes}")
      {:ok, pid} = GenServer.start_link(Gossip, 1, name: newnodeName)
      :global.register_name(newnodeName,pid)
      generateActors(totalNodes-1)
    end
  end

end


# this is the module responsible for handling all the push sum related code
defmodule PushSum do
  use GenServer
  
  def add_pushsum(pid, message, nodeId, topology, total, s, w) do
      GenServer.cast(pid, {:add_pushsum, message, nodeId, topology, total, s, w})
  end

  def checkConvergence(total, starttime, topology, isTriggered, bonusparam) do
    convergedList = ConvergenceTracker.get_convergedNode(:global.whereis_name(:"convTracker"))
    convergedTotal = Kernel.length(convergedList)
    if topology == "line" or topology == "2D" do
      convergencethreshold = 0.05
    else 
      convergencethreshold = 0.05
    end

    # add the code to randomly blacklist nodes
    connectionthreshold = convergencethreshold/2
    if convergedTotal / total >= connectionthreshold and isTriggered == false do
      # randomly snap connection for specific percentage of nodes
      isTriggered = true
      Enum.map(1..bonusparam, fn(_) ->
         bonusnode = :rand.uniform(total)
         ConvergenceTracker.add_convergedNode(:global.whereis_name(:"convTracker"), bonusnode)
      end)
    end

    if(convergedTotal / total >= convergencethreshold) do
      IO.puts "Convergence Time = #{System.system_time(:millisecond) - starttime}"
      Process.exit(self(),:kill)
    end
    checkConvergence(total, starttime, topology, isTriggered, bonusparam)
  end

  def init(messages) do
      {:ok, messages}
  end

  def handle_cast({:add_pushsum, new_message, nodeId, topology, total, halfSval, halfWval}, messages) do
    newSval = Enum.at(messages,0) + halfSval
    newWval = Enum.at(messages,1) + halfWval

    previousSWRatio = Enum.at(messages,0) / Enum.at(messages,1)
    newSWRatio = newSval / newWval
    convergenceCount = 0

    if previousSWRatio - newSWRatio < 0.0000000001 do
      if Enum.at(messages,2) == 2 do
        ConvergenceTracker.add_convergedNode(:global.whereis_name(:"convTracker"), nodeId)
      end
      convergenceCount = Enum.at(messages,2) + 1
    end

    halfSval = newSval / 2
    halfWval = newWval / 2
    newSval = newSval - halfSval
    newWval = newWval - halfWval

    updatedNodeState = [newSval, newWval, convergenceCount]

    nextNodeId = ConvergenceTracker.get_nonconvergednode(:global.whereis_name(:"convTracker"), nodeId, topology, total)
    nextnodeName = String.to_atom("node#{nextNodeId}")
    #IO.puts "node#{number} oldS = #{Enum.at(messages,0)} oldW = #{Enum.at(messages,1)} convergenceCount = #{Enum.at(messages,2)} newS = #{newS} newW = #{newW}  newCount = #{convergenceCount}"
    PushSum.add_pushsum(:global.whereis_name(nextnodeName), new_message, nextNodeId, topology, total, halfSval, halfWval)
    {:noreply, updatedNodeState}
  end

  def generateActors(totalNodes) do
    if totalNodes > 0 do
      newnodeName = String.to_atom("node#{totalNodes}")
      {:ok, pid} = GenServer.start_link(PushSum, [totalNodes,1,0], name: newnodeName)
      :global.register_name(newnodeName,pid)
      generateActors(totalNodes-1)
    end
  end
end

defmodule Project2 do
  def main(args) do
    starttime = System.system_time(:millisecond)
    :global.register_name(:"vagisha",self())
    totalNodes = String.to_integer(Enum.at(args,0))
    topology = Enum.at(args,1)
    algorithm = Enum.at(args,2)
    bonusparam = String.to_integer(Enum.at(args,3))
    if totalNodes <= bonusparam do
      IO.puts "The number of disconnecting nodes should be less than total nodes"
      Process.exit(:global.whereis_name(:"vagisha"),:kill)
    end

    if  topology == "imp2D" or topology == "2D" do
      totalNodessqrt = :math.sqrt(totalNodes) |> Float.floor |> round
      totalNodes = :math.pow(totalNodessqrt, 2) |> round
    end
    
    startNode = :rand.uniform(totalNodes)
    startnodeName = String.to_atom("node#{startNode}")
    
    if algorithm == "gossip" do
      Gossip.generateActors(totalNodes)
      {:ok, pid} = GenServer.start_link(ConvergenceTracker, [], name: :"convTracker")
      :global.register_name(:"convTracker",pid)
      :global.sync()
      Gossip.add_gossip(:global.whereis_name(startnodeName), "Exothermic", startNode, topology, totalNodes)
      Gossip.checkConvergence(totalNodes, starttime, topology, false,bonusparam)
    end
    if algorithm == "push-sum" do
      PushSum.generateActors(totalNodes)
      {:ok, pid} = GenServer.start_link(ConvergenceTracker, [], name: :"convTracker")
      :global.register_name(:"convTracker",pid)
      :global.sync()
      PushSum.add_pushsum(:global.whereis_name(startnodeName), "Push-Sum", startNode, topology, totalNodes, 0, 0)
      PushSum.checkConvergence(totalNodes, starttime, topology, false,bonusparam)
    end
  end
end
