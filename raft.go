package raft
import (
	"time"
	"fmt"
    "encoding/json"
    "math/rand"
    //"encoding/gob"
    "io/ioutil"
    "os"
    cluster "../cluster"
)
//generates random number
func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

/*Packet struct contains all 3 types of rpc appendentries, request and respince votes*/
type Packet struct{
	Term int //Term id
	Id int  //Id of the server
	LeaderId int 
	VotedId int//candidate to which vote is given
	Type string //RequestVote, ResponceVote or Appendentries
}

type Raft interface{
    Term()     int
    isLeader() bool
    Quit() 
    Start()
}


type Node struct{
	Snode cluster.Server //server node
	CurrentTerm int	//latest Term
	VotedFor int		//voted for
	L bool		//isleader
	State string //follower, leader or candidate
	ElecTout	int 	//election timeout time in millisec
	HeartBT int		//heartbeat time in millisec
	Majority int //Majority value
	Close chan bool //channel by which raft node is closed
}
type jsonobject struct {
    EToutStart int 
    EToutEnd int 
    HTout int 
    MajValue int
}
 
func (n Node) Term() int {
   return n.CurrentTerm
} 
func (n Node) isLeader() bool {
   return n.L
} 

//quite the raft server node
func (n Node) Quit() {
	n.Close <- true
}

//starts the raft server node
func (n Node) Start() {
	//n.CurrentTerm = -1
	//n.VotedFor = 0
	//n.L = false
	//n.State = "Follower"
	//SNode:=cluster.New(n.Snode.Pid(),clus_conf)
    //mynode := Node{SNode,0,0,false,"Follower",random(ES,EN),HB,maj,make(chan bool)}
    New_raft(n.Snode.Pid(),n.Majority,"peers.json","raft_conf.json")
    //go start_ser(&n)
}

func Leader(n *Node){
	select {
       case envelope := <- n.Snode.Inbox():
       		//decoding the envelope
			var dat Packet
			json.Unmarshal(envelope.Msg.([]byte),&dat)
           	switch dat.Type { 
				case "RequestVote":
					//if Term is greater than CurrentTerm, then sitdown and become follower
					if n.CurrentTerm < dat.Term{
						//set the current id
						println("Term ",n.CurrentTerm," Leader ",n.Snode.Pid()," received ReqV from ",dat.Id, " whose Term is ",dat.Term)
						n.CurrentTerm=dat.Term
						//no more leader
						n.L=false
						//convert to follower
						n.State="Follower"
					}
				default:
					println("Term ",n.CurrentTerm," Leader ",n.Snode.Pid()," received Garbage RPC")
					println(dat.Type)
			} 
		//after timeout of heartbeat send appendentries to all
       case <- time.After(time.Duration(n.HeartBT)*time.Millisecond): 
       		//sending empty AppendEntry i.e heartbeat
       		hb:=Packet{n.CurrentTerm,n.Snode.Pid(),n.Snode.Pid(),0,"AppendEntries"}
       		b, _ := json.Marshal(hb)
       		n.Snode.Outbox()<-&cluster.Envelope{Pid:-1, Msg: b}
   	}
   	return 
}

func Follower(n *Node) {
	select {
		case envelope := <- n.Snode.Inbox(): 
			//decoding the envelope
			var dat Packet
			json.Unmarshal(envelope.Msg.([]byte),&dat)
           	switch dat.Type { 
			default:
				println("Term ",n.CurrentTerm," Follower ",n.Snode.Pid()," received Garbage RPC ")
			//if request vote then see if already voted than simply send the id to which voted else check the current Term and give vote
			case "RequestVote":
				//if nil then give vote	
				if n.VotedFor==0{
					//if Term is greater than CurrentTerm then only give vote
					if n.CurrentTerm < dat.Term{
					
						//set the current id
						println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received ReqV from ",dat.Id," whose Term is ",dat.Term)
						n.CurrentTerm=dat.Term
						n.VotedFor=dat.Id
						println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," Voted for ",dat.Id," whose Term is ",dat.Term)
						//creating a responce vote envelope and sending back with the id whom the vote is given
						rv:=Packet{n.CurrentTerm,n.Snode.Pid(),0,n.VotedFor,"ResponceVote"}
						b, _ := json.Marshal(rv)
						n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg:b}
					}
				}else{
					//if Term is greater than CurrentTerm then only give vote
					if n.CurrentTerm < dat.Term{
						
						//set the current id
						println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received ReqV from ",dat.Id," whose Term is ",dat.Term)
						n.CurrentTerm=dat.Term
						n.VotedFor=dat.Id
						println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," Voted for ",dat.Id," whose Term is ",dat.Term)
						//creating a responce vote envelope and sending back with the id whom the vote is given
						rv:=Packet{n.CurrentTerm,n.Snode.Pid(),0,n.VotedFor,"ResponceVote"}
						b, _ := json.Marshal(rv)
						n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg:b}
						
					}else{
						if n.CurrentTerm == dat.Term{
							println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received ReqV ",dat.Id," whose Term is ",dat.Term)
							println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," Voted for ",n.VotedFor," whose Term is ",dat.Term)
							//creating a responce vote envelope and sending back with the id whom the vote is given
							rv:=Packet{n.CurrentTerm,n.Snode.Pid(),0,n.VotedFor,"ResponceVote"}
							b, _ := json.Marshal(rv)
							n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg: b}					
						}
					}
				}
			//if appendenrty do nothing only set the current Term
			case "AppendEntries":
				println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received AE from ",dat.LeaderId," whose Term is ",dat.Term)
				n.CurrentTerm=dat.Term
			}
		//if election timeout then become candidate		
       	case <- time.After(time.Duration(n.ElecTout) * time.Millisecond): 
       		println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," became candidate because of timeout")
       		n.State="Candidate"
   	}
   	return 
}
func Candidate(n *Node){
	//increment the Term
	n.CurrentTerm=n.CurrentTerm+1
	//vote for self
	n.VotedFor=n.Snode.Pid()
	//send RV_RPC
	println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," sending ReqV")
	//println("C",n.current)
	RV:=Packet{Term:n.CurrentTerm,Id:n.Snode.Pid(),VotedId:n.VotedFor,Type:"RequestVote"}
	b2, err1 := json.Marshal(RV)
	if err1 != nil {
		println("candidate error:", err1)
	}
	var dat1 Packet
	json.Unmarshal(b2,&dat1)
    n.Snode.Outbox()<-&cluster.Envelope{Pid:-1, Msg: b2}
    var votes int
    votes=0
	for{
	//if Majority votes comes then the candidate became leader  
	if votes >=n.Majority{
			//Majority votes so become leader
			n.State="Leader"
			println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," became leader")
			n.L=true
			return
	}
	select {	
       case envelope := <- n.Snode.Inbox(): 
       		//decoding the envelope
			var dat Packet
			json.Unmarshal(envelope.Msg.([]byte),&dat)
           	switch dat.Type { 
				default:
					println("Term ",n.CurrentTerm," Candidate ",n.Snode.Pid()," received Garbage RPC")
					return 
				case "RequestVote":
					//request came from same Term but diff candidate
					if n.CurrentTerm == dat.Term{
						println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," received ReqV from ",dat.Id," whose Term is ",dat.Term)
						//creating a responce vote envelope and sending back with the id whom the vote is given
						rv:=Packet{n.CurrentTerm,n.Snode.Pid(),0,n.VotedFor,"ResponceVote"}
						b, _ := json.Marshal(rv)
						n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg:b}
					}
				case "ResponceVote":
				//if anybody with same Term number voted then increment the count of vote
					if n.CurrentTerm==dat.Term{
						if dat.VotedId==n.Snode.Pid(){
							println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," received a vote ")
							votes=votes+1
						}
					}
				case "AppendEntries":
				//if any append entry with >= Term comes then someone has become leader so sit down
					if dat.Term >= n.CurrentTerm{
						println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," received AE from ",dat.LeaderId," whose Term is ",dat.Term)
						println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," became follower")
						n.State="Follower"
						n.CurrentTerm=dat.Term
						return
					}
			}			
				
				
       case <- time.After(time.Duration(n.ElecTout) * time.Millisecond): 
       		n.State="Candidate"
       		println("candidate timeout")
       		return 
   		}
   	}
   	return 
}

func New_raft(id int,maj int,clus_conf string,raft_conf string) *Node{
	//reading json file and storing values
	file, e := ioutil.ReadFile(raft_conf)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    } 
    var jsontype jsonobject
    json.Unmarshal(file,&jsontype)
    
    //fetching election_timeout heartbeat_timeout and Majority value
    ES:=jsontype.EToutStart
    EN:=jsontype.EToutEnd
    HB:=jsontype.HTout
    
    //making a server node
    SNode:=cluster.New(id,clus_conf)
    mynode := Node{SNode,0,0,false,"Follower",random(ES,EN),HB,maj,make(chan bool)}

    go start_ser(&mynode)
    return &mynode
}

func start_ser(n *Node){
	for{
			select {
				case msg := <-n.Close:
					if msg == true {
						println("server ",n.Snode.Pid()," closed")
						return						
					}
				default:
					switch n.State {
						case "Follower":
							Follower(n)
						case "Leader":
							Leader(n)
						case "Candidate":
							Candidate(n)
						default:
								println("unrecognized State")
						}
			}
	}
	return
}
