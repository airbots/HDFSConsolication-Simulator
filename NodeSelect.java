package edu.unl.hcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.swing.text.html.HTMLDocument.Iterator;

public class NodeSelect {
	HashMap<String, NodeInfo> nodeList;
	int clusterSize;
	int blockNum;
	HashMap<String,NodeInfo> cluster;
	int replica;
	//HashSet<NodeInfo> cluster;
	HashSet<NodeInfo> criticalNodes;
	HashSet<NodeInfo> removedNodes;
	String[][] block2Node;
	boolean rackAwareness;
	
	public NodeSelect(){}
	
	public NodeSelect(int cs, int bn, int rep,boolean raware){
		clusterSize=cs;
		blockNum=bn;
		replica=rep;
		nodeList=new HashMap<String,NodeInfo>();
		cluster=new HashMap<String,NodeInfo>();
		//cluster=new HashSet<NodeInfo>();
		criticalNodes=new HashSet<NodeInfo>();
		block2Node=new String[bn][replica];
		removedNodes=new HashSet<NodeInfo>();
		rackAwareness=raware;
	}
	
	//define custom comparator for the cluster
	public static class MyComparator implements Comparator<NodeInfo>{

		@Override
		public int compare(NodeInfo a, NodeInfo b){
			int res;
			if(!(a instanceof NodeInfo)||!(b instanceof NodeInfo)){
				res=-1;
			}
			//int res=a.getRank()
			if(a.getRank()>b.getRank()){
				res=1;
			}
			else if(a.getRank()<b.getRank()){
				res=-1;
			}
			else {
				if(a.getWeight()>b.getWeight()){
					res=1;
				}
				else if(a.getWeight()<b.getWeight()){
					res=-1;
				}
				else{
					if(a.getblockNum()>b.getblockNum()){
						res=1;
					}
					else if(a.getblockNum()<b.getblockNum()){
						res=-1;
					}
					else{
						res=a.getName().compareTo(b.getName());
					}
				}
			}
			return res;
		}
	}
	public void init(){
		System.out.println("Start to initilizing the cluster...");
		Random ran=new Random();
		int totalBlocks=blockNum*3;
		int block4Node;
		int remainBlocks;
		int nextBlock;
		
		//create the datanode in the cluster
		for(int i=0;i<clusterSize;i++){
			NodeInfo ni=new NodeInfo("node-"+i);
			nodeList.put("node-"+i, ni);
			cluster.put("node-"+i,ni);
			System.out.println("cluster contains"+cluster);
		}
		
		//if there is rackAwareness to consider
		if(this.rackAwareness==true){
				
		}
		//if there is no rackAwareness to consider
		else{
			if(clusterSize!=0 && blockNum!=0){
				for(int j=0;j<blockNum;j++){
					String nextNode="node-"+ran.nextInt(clusterSize);
				}
			}
		}
		//start to assign block to each node
		if(clusterSize!=0&& blockNum!=0){
			for(int i=0;i<blockNum;i++){
				for(int j=0;j<replica;j++){
					int nodeID=ran.nextInt(clusterSize);
					String nextNode="node-"+nodeID;
					NodeInfo ninfo=nodeList.get(nextNode);
					if(cluster.containsKey(nextNode)){
						if(!ninfo.containsBlock(i)){
							System.out.println("Will place block"+i+" "+j+"copy into node"+nextNode);
							ninfo.addBlock(i);
							block2Node[i][j]=nextNode;
						}
						else {
							System.out.println("Node "+nextNode+" has already contain block:"+i
									+"\n Continue search...");
							System.out.println("The "+nextNode+" has "+ninfo.getBlocks());
							j--;
						}
					}
					
					else{
						System.out.println("Find a node outside the cluster,"
								+ninfo+" \n Pick a node again...");
						j--;
					}
				}
			}
		}
		//random pick up nodes to assign a block
		//other method that random pick blocks and assign to nodes
		/*
		if(clusterSize!=0 && blockNum!=0){
			block4Node=totalBlocks/clusterSize;
			remainBlocks=totalBlocks%clusterSize;
			int[] budget=new int[blockNum];
			for(int j=0;j<blockNum;j++){
				budget[j]=3;
			}
			for(int i=0;i<clusterSize;i++){
				
				NodeInfo ni=new NodeInfo("Node-"+i);
				int blockonCurrentNode=block4Node;
				if(remainBlocks>0){
					blockonCurrentNode++;
					remainBlocks--;
				}
				
				//start to assign blocks to this new node
				while(blockonCurrentNode>0){
					nextBlock=ran.nextInt(blockNum);
					//System.out.println("NextBlock is "+nextBlock+"\t total block is "
						//	+blockNum+ "Block should be on this node is "+ blockonCurrentNode);
					//System.out.println("Current block "+nextBlock+" has "+budget[nextBlock]);
					if(budget[nextBlock]>0){
						if(ni.containsBlock(nextBlock)){
							//System.out.println("Find a block that has already on this node! Try again!");
							//System.out.println("Current blocks in node"+ni.getName()
							//		+" is "+ni.getBlocks().size());
							continue;
						}
						else{
							System.out.println("Assign block "+nextBlock+" to node "+ni.getName());
							if(ni.addBlock(nextBlock)){
								//ni.addBlock(nextBlock);
								block2Node[nextBlock][budget[nextBlock]-1]=ni.getName();
								budget[nextBlock]--;
							}
							else{
								System.out.println("Error in add block "+nextBlock+" to node "+ni.getName()
										+ " Will keep trying!");
								System.exit(0);
							}
						}
					}
					else{
						System.out.println("Random get a block that has no replica,try again!");
						continue;
					}
					
					blockonCurrentNode--;
					//System.out.println("Current node still need "+blockonCurrentNode+" blocks");
				}
				System.out.println("Current node "+ni.toString()+" has "+ni.getblockNum()+" blocks");
				nodeList.put(ni.getName(), ni);
				//System.out.println("The cluster size before add is"+cluster.size());
				//while(!(cluster.add(ni))){
				cluster.add(ni);
					//System.out.println("Wether cluster contain the node?"+ cluster.contains(ni)+
				//			" And its size is "+cluster.size());
				//ystem.out.println("Error in adding node "+ni.toString()+" to the cluster");
				//
				System.out.println("The cluster size after add is"+cluster.size());
			}
			updateRankandWeight();
		}
		*/
		updateRankandWeight();
		
	}
	
	public boolean remove1Node(){
		System.out.println("Start to Remove a node from the cluster...");
		Random ran=new Random();
		ArrayList<NodeInfo> nodeArray = new ArrayList<NodeInfo>();
		if(blockNum==0){
			System.out.println("The blockNum is 0, Can not remove any node!");
			return false;
		}
		else{
			NodeInfo ni;
			System.out.println("Cluster size in the remove method is"+cluster.size());
			if(cluster.size()>0){
				//System.out.println("There is not critical node, will randomly remove one");
				/*
				if(criticalNodes.size()==0){
					String nodeName="node-"+ran.nextInt(blockNum);
					//if the nodeList does not contain a nodeName, we will keep running the random number
					//generator till we find one
					while(!nodeList.containsKey(nodeName)){
						nodeName="node-"+ran.nextInt(blockNum);
					}
					ni=nodeList.get(nodeName);
				}
				//else{
				 * */
				 
					//find the next node to remove
					
					MyComparator nodeCompare=new MyComparator();
					java.util.Iterator<NodeInfo> iterator=cluster.values().iterator();
					while(iterator.hasNext()){
						nodeArray.add(iterator.next());
					}
					Collections.sort(nodeArray,nodeCompare);
					int numofCandidates=1;
					while(numofCandidates<cluster.size()
							&&((NodeInfo)nodeArray.get(0)).getRank()==((NodeInfo)nodeArray.get(numofCandidates)).getRank()
							&& ((NodeInfo)nodeArray.get(0)).getWeight()==((NodeInfo)nodeArray.get(numofCandidates)).getWeight()
							){
						numofCandidates++;
					}
					//there are more than one node has same rank and weight
					//we need using the number of resulted critical nodes to decide which
					//node need to be deleted!
					if(numofCandidates>1){
						int j=0;
						int node2Remove=0;
						int maxCriticalResultIn=0;
						while(j<numofCandidates){
							int critical=getCriticalResult((NodeInfo)nodeArray.get(j));
							if(maxCriticalResultIn<critical){
								maxCriticalResultIn=critical;
								node2Remove=j;
							}		
							j++;
						}
						ni=(NodeInfo)nodeArray.get(node2Remove);
					}
					else{
						ni=(NodeInfo)nodeArray.get(0);
					}
				//}
				//update the block2odes and criticalNodes
				HashSet hs=ni.getBlocks();
				Object[] blocks=hs.toArray();
				
				for(int i=0;i<blocks.length;i++){
					int availability=replica;
					for(int iter=0;iter<replica;iter++){
						if(block2Node[Integer.parseInt(blocks[i].toString())][iter].equals("removed")){
							availability--;
						}
						else if(block2Node[Integer.parseInt(blocks[i].toString())][iter].equals(ni.getName())){
							block2Node[Integer.parseInt(blocks[i].toString())][iter]="removed";
							availability--;							
						}

					}
					if(availability<=0){
						System.out.println("Trying to remove critical Nodes, Program will stop!");
						System.exit(0);
					}
					else if(availability==1){
						NodeInfo newCnode;
						boolean findNode=false;
						for(int iter=0;iter<replica;iter++){
							if(!(block2Node[Integer.parseInt(blocks[i].toString())][iter].equals("removed"))){
								newCnode=nodeList.get(block2Node[Integer.parseInt(blocks[i].toString())][iter]);
								criticalNodes.add(newCnode);
								if(cluster.containsKey(newCnode.getName())){
								    cluster.remove(newCnode.getName());
								}
								findNode=true;
							}
						}
						if(!findNode){
							System.out.println("Can not Find the newly generated Critical nodes!" +
									"Program will exits");
							System.exit(0);
						}
					}
				}
				
				//remove this node from cluster and add it to the removedNodes set
				System.out.println("System will remove the node "+ni.getName());
				removedNodes.add(ni);
				System.out.println("The node will be removed:"+ni);
				System.out.println("cluster before removal:"+cluster);
			    while(cluster.remove(ni.getName())==null){
			    	System.out.println("Can not remove node"+ni.getName());
			    }
				System.out.println("cluster after removal:"+cluster);
				//update rank and weight
				updateRankandWeight();
				return true;
			}
			else{
				System.out.println("You can not remove any node from current cluster!"
						+ "Cluster size is "+cluster.size());
			}
			return false;
		}
	}
	
	
	
	public int getCriticalResult(NodeInfo node){
		
		HashSet hs=node.getBlocks();
		Object[] blocks=hs.toArray();
		HashSet<NodeInfo> possibleCriticalNode=new HashSet<NodeInfo>();
		int availability=3;
		try{
			for(int i=0;i<blocks.length;i++){
				availability=3;
				for(int iter=0;iter<3;iter++){
					if(block2Node[Integer.parseInt(blocks[i].toString())][iter].equals("removed")){
						availability--;
					}
				}
				if(availability==1){
					if(criticalNodes.contains(node)){
						continue;
					}
					else possibleCriticalNode.add(node);	
				}
				else if(availability==0){
					System.out.println("Error remove a critical node, Program exit!");
					System.exit(0);
				}
				else continue;
			}
			
			
		}catch (Exception e){
			System.out.println("Error in get critical Result method!");
			e.printStackTrace();
		}
		return possibleCriticalNode.size();
	}
	
	public void updateRankandWeight(){
		System.out.println("Start to update the node Rank and weight...");
		System.out.println("The cluster has "+cluster.size()+" nodes");
		Object[] nodeArray=cluster.values().toArray();
		for(int i=0;i<nodeArray.length;i++){
			NodeInfo nio=(NodeInfo)nodeArray[i];
			//System.out.println("Old weight is "+nio.getWeight()+" old rank is "+nio.getRank());
			int oldRank=nio.getRank();
			int oldWeight=nio.getWeight();
			int newRank=0;
			int newWeight;
			int availability;
			Object[] blockin1Node=nio.getBlocks().toArray();
			System.out.println("The node "+ nio.getName()+" has "+blockin1Node.length+" blocks");
			newWeight=blockin1Node.length;
			for(int j=0;j<blockin1Node.length;j++){
				availability=replica;
				for(int k=0;k<replica;k++){
					//System.out.println("row"+Integer.parseInt(blockin1Node[j].toString())+"column"+k
						//	+"value"+block2Node[Integer.parseInt(blockin1Node[j].toString())][k]);
					if(block2Node[Integer.parseInt(blockin1Node[j].toString())][k].equals("removed")){
						//System.out.println("Find a block has been removed!");
						availability--;
					}
				}
				if(availability==2){
					newRank++;
				}
			}
			if(newRank>=oldRank){
				nio.setRank(newRank);
			}
			//if(newWeight>=oldWeight){
			nio.setWeight(newWeight);
			//}
			//System.out.println("New weight is "+nio.getWeight()+" new rank is "+nio.getRank());
		}
	}
	
	/*
	public void updateWeight(NodeInfo node){
		
	}
	*/
	
	//if the rank and weight are all the same, we need to find a node that can result in smallest
	//number of critical nodes
	public NodeInfo findNextNode(){
		return null;
	}
	
	public static void main(String[] args){
		int numofNodes=0;
		int numofBlocks=0;
		int numofReplica=0;
		boolean rackaware=false;
		
		//parse the input parameters
		for(int i=0;i<args.length;i++){
			if(args[i].equals("-node")){
				numofNodes=Integer.parseInt(args[++i]);
			}
			else if(args[i].equals("-block")){
				numofBlocks=Integer.parseInt(args[++i]);
			}
			else if(args[i].equals("-rack")){
				rackaware=(args[++i].equals("true"))?true:false;
			}
			else if(args[i].equals("-replica")){
				numofReplica=Integer.parseInt(args[++i]);
				if(numofReplica<1){
					numofReplica=3;
				}
			}
		}
		//check the cluster size and the block number
		if(!(numofNodes>0 && numofBlocks>0)){
			System.out.println("Please input the cluster size and the number of blocks!");
			System.exit(-1);
		}
		else{
			NodeSelect ns=new NodeSelect(numofNodes,numofBlocks,numofReplica,rackaware);
			System.out.println("Will start to assign "+numofBlocks +" blocks to "
					+numofNodes+" nodes cluster");
			ns.init();
			while(ns.cluster.size()>0){
				if(ns.remove1Node()){
					System.out.println("Current Cluster size is"+ns.cluster.size());
				}
				else{
					System.out.println("Can not remove node any more from the cluster!\n" +
							"Cluster size is"+ns.cluster.size()+" Program will exit!");
					break;
				}
			}
			//output the critical set
			Object[] niarray= ns.criticalNodes.toArray();
			System.out.println("Program will print out "+niarray.length+" critical Nodes:");
			for(int k=0;k<niarray.length;k++){
				NodeInfo ninfo=(NodeInfo)niarray[k];
				Object[] blockarray=ninfo.blocks.toArray();
				System.out.println("Node"+ninfo.getName()+" :\t");
				for(int j=0;j<blockarray.length;j++){
					System.out.print(blockarray[j]+" ");
				}
			}
		}
		System.out.println("This is the end of the program");
	}
}
