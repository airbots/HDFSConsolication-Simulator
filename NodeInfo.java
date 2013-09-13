package edu.unl.hcc;

import java.util.HashSet;


public class NodeInfo {

	String hostname;
	HashSet<Integer> blocks;
	int rank;
	int weight;
	
	//default ctor
	public NodeInfo(){
		blocks=new HashSet();
	}
	
	public NodeInfo(String name){
		if(!name.equals(null)){
			hostname=name;
		}
		blocks=new HashSet();
		rank=0;
		weight=0;
	}
	
	public int getRank(){
		return rank;
	}
	
	public int getWeight(){
		return weight;
	}
	
	public void setRank(int ra){
			rank=ra;
	}
	
	public void setWeight(int wt){
		weight=wt;
	}
	
	public boolean addBlock(int blockID){
		if(blockID>-1){
			return blocks.add(blockID);
		}
		else {
			System.out.println("Failed to add block:"+blockID);
			return false;
		}
	}
	
	public String getName(){
		if(!hostname.equals(null)){
			return hostname;
		}
		else return null;
	}
	
	public HashSet getBlocks(){
		return this.blocks;
	}
	
	public boolean setHostName(String hn){
		if(!hostname.equals(null)){
			this.hostname=hn;
			return true;
		}
		else return false;
	}
	
	
	public boolean containsBlock(int blockID){
		if(blocks.contains(blockID)){
			return true;
		}
		else return false;
	}
	
	public int getblockNum(){
		return blocks.size();
	}
	
	public boolean removeBlock(int blockID){
		if(blocks.contains(blockID)){
			return blocks.remove(blockID);
		}
		
		else {
			System.out.println("Does not contain this block with ID"+ blockID);
			return false;
		}
	}
	
	
}
