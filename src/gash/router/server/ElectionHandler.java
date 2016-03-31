package gash.router.server;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import pipe.common.Common;
import pipe.work.Work;

import java.beans.Beans;

/**
 * Created by patel on 3/30/2016.
 */
public class ElectionHandler {

    private int numberOfAttempts = 3;

    private int leaderNodeId = -1;

    private Election election;

    private Integer syncInt = 0;

    private static RoutingConf conf;

    private static ElectionHandler electionHandlerInstance;

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
            if(leaderNodeId < 0)
            {
                System.out.print("Time for election!!!");
                // do the leader election
            }
        }
    }

    public void askWhoIsTheLeader()
    {
        Work.WorkState.Builder sb = Work.WorkState.newBuilder();
        sb.setEnqueued(-1);
        sb.setProcessed(-1);

        pipe.election.Election.LeaderStatus.Builder leaderStatusBuilder = pipe.election.Election.LeaderStatus.newBuilder();

        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(conf.getNodeId());
        hb.setDestination(-1);
        hb.setTime(System.currentTimeMillis());

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb);
        wb.setLeader(leaderStatusBuilder);

        wb.setSecret(1000l);

        EdgeMonitor.broadcastMessage(wb);
//        return wb.build();
    }

    public int getLeaderNodeId()
    {
        return leaderNodeId;
    }

    private Election electionInstance() {
        if (election == null) {
            synchronized (syncInt) {
                if (election !=null)
                    return election;

                try {

                    // create Election class

                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.print("Failed to get election instance");
                }
            }
        }

        return election;

    }
}
