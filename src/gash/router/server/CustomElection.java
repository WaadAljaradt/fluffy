package gash.router.server;

import pipe.common.Common;
import pipe.election.Election;
import pipe.work.Work;

import java.util.Date;
import java.util.List;

/**
 * Created by patel on 3/31/2016.
 */
public class CustomElection {

    private int nodeId;
//    private ElectionState current;
    private int maxHops = -1; // unlimited
//    private ElectionListener listener;
    protected Election.LeaderElection.ElectAction state = Election.LeaderElection.ElectAction.DECLAREELECTION;
    protected long electionID;
    protected long startedOn = 0, lastVoteOn = 0, maxDuration = -1;
    protected int candidateId;

    private boolean hasAlreadyVoted = false;

    protected boolean active = false;

    protected int receivedVotes = 0;
    protected int maxVotes = 0;

    public CustomElection(int nodeId) {
        this.nodeId = nodeId;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isElectionInprogress()
    {
        return active;
    }

    public long getElectionId() {
        return electionID;
    }

    private void setElectionId(int id) {
        electionID = id;
    }

    public synchronized void clear() {
        active = false;
    }

    public boolean updateCurrent(Election.LeaderElection req) {

        boolean isNewElection = false;

        if(!active)
            isNewElection = true;

        maxVotes = req.getHops();	// Holds the no. of max favorable votes required

        electionID = req.getElectionId();
        candidateId = req.getCandidateId();
        maxDuration = req.getExpires();
        startedOn = System.currentTimeMillis();
        state = req.getAction();
        active = true;

        return isNewElection;
    }

    public Integer getWinner() {
        if (state.getNumber() == Election.LeaderElection.ElectAction.DECLAREELECTION_VALUE)
            return candidateId;
        else
            return null;
    }

    public void setMaxHops(int maxHops) {
        this.maxHops = maxHops;
    }

    public Work.WorkMessage process(Work.WorkMessage message) {
        if (!message.hasElection())
            return null;

        Election.LeaderElection req = message.getElection();

        Work.WorkMessage toReturnMessage = null;

        if (req.getAction().getNumber() == Election.LeaderElection.ElectAction.DECLAREELECTION_VALUE) {
            // an election is declared!

            // required to eliminate duplicate messages - on a declaration,
            // should not happen if the network does not have cycles
//            List<VectorClock> rtes = message.getHeader().getPathList();
//            for (VectorClock rp : rtes) {
//                if (rp.getNodeId() == this.nodeId) {
//                    // message has already been sent to me, don't use and
//                    // forward
//                    return null;
//                }
//            }

            if(hasAlreadyVoted)
            {
                return null;
            }

            boolean isNew = updateCurrent(req);
            toReturnMessage = castVote(message, isNew);

        } else if (req.getAction().getNumber() == Election.LeaderElection.ElectAction.DECLAREVOID_VALUE) {
            System.out.println("no one was elected");
            this.clear();
            ElectionHandler.getInstance().concludeWith(false, null);
        } else if (req.getAction().getNumber() == Election.LeaderElection.ElectAction.DECLAREWINNER_VALUE) {
            System.out.println("Election " + req.getElectionId() + ": Node " + req.getCandidateId() + " is declared the leader");
            updateCurrent(message.getElection());
            active = false;
            ElectionHandler.getInstance().concludeWith(true, req.getCandidateId());
        } else if (req.getAction().getNumber() == Election.LeaderElection.ElectAction.NOMINATE_VALUE) {
            toReturnMessage = castVote(message, false);
        }

        return toReturnMessage;
    }

    private synchronized Work.WorkMessage castVote(Work.WorkMessage message, boolean isNew) {
        if (!message.hasElection())
            return null;

        if (!active) {
            return null;
        }

        Election.LeaderElection req = message.getElection();
        if (req.getExpires() <= System.currentTimeMillis()) {
            // election expired so not voting
            return null;
        }

        System.out.println("casting vote in election " + req.getElectionId()+ " at stage "+ req.getAction().getNumber());

        // DANGER! If we return because this node ID is in the list, we have a
        // high chance an election will not converge as the maxHops determines
        // if the graph has been traversed!
//        boolean allowCycles = true;
//
//        if (!allowCycles) {
//            List<VectorClock> rtes = mgmt.getHeader().getPathList();
//            for (VectorClock rp : rtes) {
//                if (rp.getNodeId() == this.nodeId) {
//                    // logger.info("Node " + this.nodeId +
//                    // " already in the routing path - not voting");
//                    return null;
//                }
//            }
//        }

        if(hasAlreadyVoted )
            return null;

        // okay, the message is new (to me) so I want to determine if I should
        // nominate myself

        Election.LeaderElection.Builder leaderElectionBuilder = Election.LeaderElection.newBuilder();

        Common.Header.Builder hb = Common.Header.newBuilder();
        hb.setNodeId(message.getHeader().getNodeId());
        hb.setDestination(-1);
        hb.setTime(System.currentTimeMillis());

//        Work.WorkMessage.Builder mhb = Work.WorkMessage.newBuilder();
//        mhb.setTime(System.currentTimeMillis());
//        mhb.setSecurityCode(-999); // TODO add security
//        mhb.setOriginator(mgmt.getHeader().getOriginator());

        leaderElectionBuilder.setElectionId(req.getElectionId());

        if(req.getAction().getNumber() == Election.LeaderElection.ElectAction.DECLAREELECTION_VALUE){
            leaderElectionBuilder.setAction(Election.LeaderElection.ElectAction.NOMINATE);
            leaderElectionBuilder.setCandidateId(req.getCandidateId());
            if (req.getCandidateId() < this.nodeId){
                leaderElectionBuilder.setCandidateId(this.nodeId);
                candidateId = this.nodeId;
            }
        }
        else if(req.getAction().getNumber() == Election.LeaderElection.ElectAction.NOMINATE_VALUE){
            receivedVotes++;
            if(req.getCandidateId()> candidateId){
                candidateId = req.getCandidateId();
            }
            if(receivedVotes == req.getHops()){
                leaderElectionBuilder.setCandidateId(candidateId);
                leaderElectionBuilder.setAction(Election.LeaderElection.ElectAction.DECLAREWINNER);
                ElectionHandler.getInstance().concludeWith(true, candidateId);
            }
            else{
                return null;
            }

        }

        leaderElectionBuilder.setExpires(req.getExpires());

        //int totalConns = ConnectionManager.getNumMgmtConnections();
        leaderElectionBuilder.setHops(req.getHops());

//        // add myself (may allow duplicate entries, if cycling is allowed)
//        VectorClock.Builder rpb = VectorClock.newBuilder();
//        rpb.setNodeId(this.nodeId);
//        rpb.setTime(System.currentTimeMillis());
//        rpb.setVersion(req.getElectId());
//        mhb.addPath(rpb);
        hasAlreadyVoted = true;

        Work.WorkMessage.Builder wb = Work.WorkMessage.newBuilder();
        wb.setHeader(hb);
        wb.setElection(leaderElectionBuilder);

        return wb.build();
    }
}
