package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc;
import edu.stevens.cs549.dht.rpc.Id;
import edu.stevens.cs549.dht.rpc.Key;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeBindings;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ClientCall;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private IChannels channels;

	private WebClient(IChannels channels) {
		this.channels = channels;
	}

	public static WebClient getInstance(IState state) {
		return new WebClient(state.getChannels());
	}

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	/*
	 * Get a blocking stub (channels and stubs are cached for reuse).
	 */
	private DhtServiceGrpc.DhtServiceBlockingStub getStub(String targetHost, int targetPort) {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceGrpc.DhtServiceBlockingStub getStub(NodeInfo target) {
		return getStub(target.getHost(), target.getPort());
	}


	/*
	 * TODO: Fill in missing operations.
	 */

	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getPred(" + node.getId() + ")");
		try {
			OptNodeInfo pred = getStub(node).getPred(Empty.getDefaultInstance());
			if (pred == null) {
				throw new DhtBase.Failed("getPred() RPC returned null for node " + node.getId());
			}
			return pred;
		} catch (Exception e) {
			error("getPred() RPC failed", e);
			throw new DhtBase.Failed("getPred() RPC failed");
		}
	}


	public NodeInfo getSucc(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getSucc(" + node.getId() + ")");
		try {
			NodeInfo succ = getStub(node).getSucc(Empty.getDefaultInstance());
			if (succ == null) {
				throw new DhtBase.Failed("getSucc() RPC returned null for node " + node.getId());
			}
			return succ;
		} catch (Exception e) {
			error("getSucc() RPC failed", e);
			throw new DhtBase.Failed("getSucc() RPC failed");
		}
	}


	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) throws DhtBase.Failed {
		Log.weblog(TAG, "closestPrecedingFinger(" + node.getId() + ", " + id + ")");
		try {
			Id req = Id.newBuilder().setId(id).build();
			NodeInfo cpf = getStub(node).closestPrecedingFinger(req);
			if (cpf == null) {
				throw new DhtBase.Failed("closestPrecedingFinger() RPC returned null for node " + node.getId());
			}
			return cpf;
		} catch (Exception e) {
			error("closestPrecedingFinger() RPC failed", e);
			throw new DhtBase.Failed("closestPrecedingFinger() RPC failed");
		}
	}


	public String[] get(NodeInfo node, String key) throws DhtBase.Failed {
		try {
			Key req = Key.newBuilder().setKey(key).build();
			Bindings resp = getStub(node).getBindings(req);
			return resp.getValueList().toArray(new String[0]);
		} catch (Exception e) {
			error("get() failed", e);
			throw new DhtBase.Failed("get() RPC failed");
		}
	}

	public void add(NodeInfo node, String key, String value) throws DhtBase.Failed {
		try {
			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
			getStub(node).addBinding(b);
		} catch (Exception e) {
			error("add() failed", e);
			throw new DhtBase.Failed("add() RPC failed");
		}
	}

	public void delete(NodeInfo node, String key, String value) throws DhtBase.Failed {
		try {
			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
			getStub(node).deleteBinding(b);
		} catch (Exception e) {
			error("delete() failed", e);
			throw new DhtBase.Failed("delete() RPC failed");
		}
	}

	public NodeInfo findSuccessor(NodeInfo node, int id) throws DhtBase.Failed {
		try {
			Id req = Id.newBuilder().setId(id).build();
			NodeInfo succ = getStub(node).findSuccessor(req);
			if (succ == null) {
				throw new DhtBase.Failed("findSuccessor() RPC returned null for node " + node.getId());
			}
			return succ;
		} catch (Exception e) {
			error("findSuccessor() failed", e);
			throw new DhtBase.Failed("findSuccessor() RPC failed");
		}
	}



	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		// TODO
		// throw new IllegalStateException("notify() not yet implemented");
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null.
		 */
		try{
			Log.weblog(TAG, "notify(" + node.getId()+")");
			return getStub(node).notify(predDb);
		}
		catch(Exception e)
		{
			error("notify() failed, try again", e);
			throw new DhtBase.Failed("notify() RPC failed, do better next time");
		}
		//return null;
	}


}
