package gash.router.server;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;
import pipe.common.Common;
import pipe.election.Election;
import pipe.work.Work;

/**
 * Created by patel on 3/30/2016.
 */
public class ElectionHandler {

    private int numberOfAttempts = 3;

    private int leaderNodeId = -1;

    private CustomElection customElection;

    private Integer syncInt = 0;

    private static RoutingConf conf;

    private static ElectionHandler electionHandlerInstance;

    private long electionId;

    public static void init(RoutingConf cf)
    {
        conf = cf;
    }

    public static synchronized ElectionHandler getInstance(){
        if(electionHandlerInstance == null)
            electionHandlerInstance = new ElectionHandler();

        return electionHandlerInstance;
    }


    public void checkCurrentState()
    {
        if(numberOfAttempts > 0 && EdgeMonitor.activeConnections.size() > 0)
        {
            // ask who is the leader
            numberOfAttempts--;
            askWhoIsTheLeader();
        }
        else
        {
            if(leaderNodeId < 0 && (customElection == null || !customElection.isElectionInprogress()))
            {
                System.out.print("Time for customElection!!!");
                // do the leader customElection
                synchronized (syncInt)
                {
                    startElection();
                }
            }
//            else
//            {
//                if(leaderNodeId >= 0)
//                    System.out.println("The leader is " + leaderNodeId);
//                else
//                    System.out.println("Elecion in progress");
//            }
        }
    }

    public synchronized void startElection()
    {
        electionId = System.currentTimeMillis();

        getElectionInstance().active = true;
        getElectionInstance().hasAlreadyVoted = false;

        Election.LeaderElection.Builder leaderElectionBuilder = Election.LeaderElection.newBuilder();
        leaderElectionBuilder.setElectionId(electionId);
        leaderElectionBuilder.setAction(Election.LeaderElection.ElectAction.DECLAREELECTION);
        leaderElectionBuilder.setCandidateId(conf.getNodeId());
        leaderElectionBuilder.setExpires(2 * 60 * 1000 + System.currentTimeMillis());
        leaderElectionBuilder.setHops(EdgeMonitor.activeConnections.size());

        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(conf.getNodeId());
        hb.setDestination(-1);
        hb.setTime(System.currentTimeMillis());

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb);
        wb.setElection(leaderElectionBuilder);

        wb.setSecret(1000l);

        EdgeMonitor.broadcastMessage(wb.build());
    }

    public void askWhoIsTheLeader()
    {
        Work.WorkState.Builder sb = Work.WorkState.newBuilder();
        sb.setEnqueued(-1);
        sb.setProcessed(-1);

        pipe.election.Election.LeaderStatus.Builder leaderStatusBuilder = pipe.election.Election.LeaderStatus.newBuilder();
        leaderStatusBuilder.setAction(pipe.election.Election.LeaderStatus.LeaderQuery.WHOISTHELEADER);
//        leaderStatusBuilder.setLeaderHost("");
//        leaderStatusBuilder.setLeaderId(-1);

        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(conf.getNodeId());
        hb.setDestination(-1);
        hb.setTime(System.currentTimeMillis());

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb);
        wb.setLeader(leaderStatusBuilder);

        wb.setSecret(1000l);

        EdgeMonitor.broadcastMessage(wb.build());
//        return wb.build();
    }

    private void whoIsTheLeader(Work.WorkMessage message) {

        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(conf.getNodeId());
        hb.setDestination(message.getHeader().getNodeId());
        hb.setTime(System.currentTimeMillis());

        pipe.election.Election.LeaderStatus.Builder leaderStatusBuilder = pipe.election.Election.LeaderStatus.newBuilder();
        leaderStatusBuilder.setAction(Election.LeaderStatus.LeaderQuery.THELEADERIS);
        leaderStatusBuilder.setElectionId(electionId);
        leaderStatusBuilder.setLeaderId(leaderNodeId);

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb);
        wb.setLeader(leaderStatusBuilder);

        wb.setSecret(1000l);

        try {
                EdgeInfo ei = EdgeMonitor.activeConnections.get(message.getHeader().getNodeId());
                if(ei.getChannel() != null && ei.getChannel().isOpen())
                {
                    ei.getChannel().writeAndFlush(wb.build());
                }

//            ConnectionManager.getConnection(mgmt.getHeader().getOriginator(), true).write(mb.build());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public int getLeaderNodeId()
    {
        return leaderNodeId;
    }

    public synchronized int leaderIsDead()
    {
        if(leaderNodeId == conf.getNodeId())
        {
            numberOfAttempts = 3;
        }
        return leaderNodeId = -1;
    }

    private synchronized CustomElection getElectionInstance() {
        if (customElection == null) {
            synchronized (syncInt) {
                if (customElection !=null)
                    return customElection;

                try {

                    // create CustomElection class
                    customElection = new CustomElection(conf.getNodeId());

                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.print("Failed to get customElection instance");
                }
            }
        }

        return customElection;

    }

    public synchronized void handleElection(Work.WorkMessage electionMessage)
    {
        try {

            getElectionInstance().active = true;

            if (electionMessage.getElection().hasExpires()) {
                long ct = System.currentTimeMillis();
                if (ct > electionMessage.getElection().getExpires()) {
                    // ran out of time so the election is over
                    System.out.println("Election expired");
                    getElectionInstance().clear();
                    return;
                }
            }

            if((electionId == -1) || (electionId== electionMessage.getElection().getElectionId())){		// This node has not started the election, should participate in the current election
//            logger.info("Processing the first election received..");
            }
            else if(electionMessage.getElection().getElectionId() < electionId){	// Means the node received an election which was started before the one the node is processing
//            logger.info("Received an older election..clearing the previous election as we are considering the oldest election");
                getElectionInstance().clear();   // Clear the previous election state
                System.out.println("Clearing own election before older election found");
            }
            electionId = electionMessage.getElection().getElectionId();

            System.out.println("Processing Election Message...");

            Work.WorkMessage toReturnMessage = getElectionInstance().process(electionMessage);
            if (toReturnMessage != null){

                try {
                    if(toReturnMessage.getElection().getAction().getNumber() == Election.LeaderElection.ElectAction.DECLAREWINNER_VALUE) {

                        System.out.println("Declaring winner");

                        EdgeMonitor.broadcastMessage(toReturnMessage);
                    }else {

                        System.out.println("Sending response");

                        EdgeMonitor.sendMessage(electionMessage.getHeader().getNodeId(), toReturnMessage);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void handleLeader(Work.WorkMessage message)
    {
        if(message.getLeader().getAction() == Election.LeaderStatus.LeaderQuery.WHOISTHELEADER) {
            whoIsTheLeader(message);
        }
        else if(message.getLeader().getAction() == Election.LeaderStatus.LeaderQuery.THELEADERIS) {
            leaderNodeId = message.getLeader().getLeaderId();
        }
    }

    public void concludeWith(boolean success, Integer leaderID) {
        if (success) {
            System.out.println("The leader is " + leaderID);
            leaderNodeId = leaderID;
        }

        customElection.clear();
        electionId = -1;
    }
}
