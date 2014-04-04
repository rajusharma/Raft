package raft
import (
	"time"
    //"math/rand"
    "testing"
    //"encoding/gob"
)

//set the majority value 
var MAJ int = 3

func Test_raft(t *testing.T){
	//making Raft servers and putting in array
   	var raft_arr []Raft
   	//pid starts from 10-14
  	p:=10
  	//starting all 5 raft servers
  	for i:=1;i<6;i++{
   		raft_arr=append(raft_arr,New_raft(p,MAJ,"peers.json","raft_conf.json"))
   		p++
   	}
   	
   	<-time.After(4*time.Second)
   	
   	
   	var numofLeader = 0
   	var isleader = 0
   	var quitted = 0
   	var term = 0
   	////////////////////////////////////////////////////////////////////////////////////////////////
   	//Test 1 -checking only 1 leader is selected
   	for i:=0;i<5;i++{
   		if raft_arr[i].isLeader() {
   			isleader = i
   			numofLeader++
   			term = raft_arr[i].Term()
   		}
   	}
   	
   	//if there is only one leader test passed
   	if numofLeader ==1{
   		println("Test 1 passed ",raft_arr[isleader].Id()," is new leader")
   	}
   	
   	//<-time.After(2*time.Second)
   	
   	//////////////////////////////////////////////////////////////////////////////////////////
   	//Test 2 - shutting the leader node and see other node nodes becomes leader or not in next term
   	numofLeader = 0
   	println("Term ",term," ",raft_arr[isleader].Id()," is shutting down")
   	
   	//shutting down the leader
   	raft_arr[isleader].Quit()
   	quitted = isleader
   	
   	<-time.After(4*time.Second)
   	
   	//checking who is new leader
   	for i:=0;i<5;i++{
   		if raft_arr[i].isLeader() {
   			isleader = i
   			numofLeader++
   			term = raft_arr[i].Term()
   		}
   	}
   	
   	//if there is only one leader test passed
   	if numofLeader ==1{
   		println("Test 2 passed",raft_arr[isleader].Id()," is new leader")
   	}
   	
   	//starting again the quitted server
   	raft_arr[quitted].Start()
   	
   	<-time.After(3*time.Second)
   	
   ///////////////////////////////////////////////////////////////////////////////////////
   	//Test 3 - shutting any 2 node and see 
   	numofLeader = 0
   	
   	//setting the leader to follower first
   	raft_arr[isleader].Reset()
   	
   	println("Term ",term," ",raft_arr[0].Id()," is shutting down")
   	println("Term ",term," ",raft_arr[1].Id()," is shutting down")
   	
   	//shutting down
   	raft_arr[0].Quit()
   	raft_arr[1].Quit()
   	
   	<-time.After(4*time.Second)
   	
   	//checking who is new leader
   	for i:=0;i<5;i++{
   		if raft_arr[i].isLeader() {
   			isleader = i
   			numofLeader++
   			term = raft_arr[i].Term()
   		}
   	}
   	
   	//if there is only one leader test passed
   	if numofLeader ==1{
   		println("Test 3 passed",raft_arr[isleader].Id()," is new leader")
   	}
   	
   	//starting again the quitted server
   	raft_arr[0].Start()
   	raft_arr[1].Start()
   	
   	<-time.After(3*time.Second)
   	
   	///////////////////////////////////////////////////////////////////////////////////////
   	//Test 4 - shutting any 3 node
   	numofLeader = 0
   	println("Term ",term," ",raft_arr[0].Id()," is shutting down")
   	println("Term ",term," ",raft_arr[1].Id()," is shutting down")
   	println("Term ",term," ",raft_arr[2].Id()," is shutting down")
   	
   	//shutting down 
   	raft_arr[0].Quit()
   	raft_arr[1].Quit()
   	raft_arr[2].Quit()
   	
   	<-time.After(4*time.Second)
   	
   	//checking who is new leader
   	for i:=0;i<5;i++{
   		if raft_arr[i].isLeader() {
   			isleader = i
   			numofLeader++
   			term = raft_arr[i].Term()
   		}
   	}
   	
   	//if no one is leader 
   	if numofLeader ==0{
   		println("No one become leader")
   		println("Test 4 passed no one is leader")
   	}
   	
   	//<-time.After(4*time.Second) 		
   return
}
