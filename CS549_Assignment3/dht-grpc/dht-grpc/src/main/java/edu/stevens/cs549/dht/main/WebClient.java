package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.events.EventConsumer;
import edu.stevens.cs549.dht.events.IEventListener;
import edu.stevens.cs549.dht.rpc.Binding;
import edu.stevens.cs549.dht.rpc.Bindings;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceBlockingStub;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceStub;
import edu.stevens.cs549.dht.rpc.Id;
import edu.stevens.cs549.dht.rpc.Key;
import edu.stevens.cs549.dht.rpc.NodeBindings;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.rpc.OptNodeBindings;
import edu.stevens.cs549.dht.rpc.OptNodeInfo;
import edu.stevens.cs549.dht.rpc.Subscription;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;
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
	private DhtServiceBlockingStub getStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceBlockingStub getStub(NodeInfo target) throws DhtBase.Failed {
		return getStub(target.getHost(), target.getPort());
	}

	private DhtServiceStub getListenerStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newStub(channel);
	}

	private DhtServiceStub getListenerStub(NodeInfo target) throws DhtBase.Failed {
		return getListenerStub(target.getHost(), target.getPort());
	}


	/*
	 * TODO: Fill in missing operations.
	 */

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


	public Bindings getBindings(NodeInfo node, String key) throws DhtBase.Failed {
		Log.weblog(TAG, "getBindings(" + node.getId() + ", " + key + ")");
		Key request = Key.newBuilder().setKey(key).build();
		return getStub(node).getBindings(request);
	}


	public Empty addBinding(NodeInfo node, Binding binding) throws DhtBase.Failed {
		Log.weblog(TAG, "addBinding(" + node.getId() + ", key=" + binding.getKey() + ", val=" + binding.getValue() + ")");
		try {
			return getStub(node).addBinding(binding);
		} catch (Exception e) {
			error("addBinding() failed", e);
			throw new DhtBase.Failed("addBinding() RPC failed");
		}
	}

//	public void add(NodeInfo node, String key, String value) throws DhtBase.Failed {
//		try {
//			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
//			//NodeInfo correctNode = findSuccessor(node, key.hashCode());
//			getStub(node).addBinding(b);
//		} catch (Exception e) {
//			error("add() failed", e);
//			throw new DhtBase.Failed("add() RPC failed");
//		}
//	}

//	public void add(NodeInfo node, String key, String value) throws DhtBase.Failed {
//		try {
//			int keyId = DhtBase.NodeKey(key);
//			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
//			NodeInfo correctNode = findSuccessor(node, keyId);
//			addBinding(correctNode, b);
//
//		} catch (Exception e) {
//			error("add() failed", e);
//			throw new DhtBase.Failed("add() RPC failed");
//		}
//	}

	public void add(NodeInfo node, String key, String value) throws DhtBase.Failed {
		try {
			int keyId = DhtBase.NodeKey(key);
			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
			if (node.getId() == keyId) {
				//System.out.println("Key '" + key + "' matches node ID " + node.getId() + " — calling addBinding directly.");
				addBinding(node, b);
				return;
			}
			NodeInfo correctNode = findSuccessor(node, keyId);
			addBinding(correctNode, b);

		} catch (Exception e) {
			error("add() failed", e);
			throw new DhtBase.Failed("add() RPC failed");
		}
	}



//	public void add(NodeInfo node, String key, String value) throws DhtBase.Failed {
//		try {
//			int keyId = edu.stevens.cs549.dht.activity.DhtBase.NodeKey(key);
//			NodeInfo correctNode = findSuccessor(node, keyId);
//			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
//			addBinding(correctNode, b);
//		} catch (Exception e) {
//			error("add() failed", e);
//			throw new DhtBase.Failed("add() RPC failed");
//		}
//	}

//	public void add(NodeInfo caller, String key, String value) throws DhtBase.Failed {
//		try {
//			int keyId = edu.stevens.cs549.dht.activity.DhtBase.NodeKey(key);
//			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
//
//			// Find the node responsible for this key
//			NodeInfo correctNode = findSuccessor(caller, keyId);
//			System.out.println("Routing key '" + key + "' (id=" + keyId + ") to node " + correctNode.getId());
//
//			// Attempt to store at the correct node
//			addBinding(correctNode, b);
//
//		} catch (Error invalid) {
//			System.out.println("ERROR: Node rejected key: " + key);
//			throw new DhtBase.Failed("Invalid key placement: " + key);
//		} catch (Exception e) {
//			error("add() failed", e);
//			throw new DhtBase.Failed("add() RPC failed");
//		}
//	}


//	public void add(NodeInfo node, String key, String value) throws DhtBase.Failed {
//		try {
//			int keyId = edu.stevens.cs549.dht.activity.DhtBase.NodeKey(key);
//			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
//
//			NodeInfo successor = findSuccessor(node, keyId);
//			OptNodeInfo pred = getPred(successor);
//
//			try {
//				addBinding(successor, b);
//			} catch (Exception e1) {
//				System.out.println("WARNING: Successor " + successor.getId() + " rejected key " + key + " (id=" + keyId + "). Trying fallback options.");
//
//				// Try predecessor only if it's different from self
//				try {
//					NodeInfo predecessor = pred.getNodeInfo();
//					if (predecessor != null && predecessor.getId() != node.getId()) {
//						addBinding(predecessor, b);
//					} else {
//						addBinding(node, b); // last resort
//					}
//				} catch (Exception e2) {
//					System.out.println("ERROR: All fallbacks failed for key " + key);
//					throw new DhtBase.Failed("All attempts to add key failed: " + key);
//				}
//			}
//
//		} catch (Exception e) {
//			error("add() failed", e);
//			throw new DhtBase.Failed("add() RPC failed");
//		}
//	}

	public void delete(NodeInfo node, String key, String value) throws DhtBase.Failed {
		try {
			Binding b = Binding.newBuilder().setKey(key).setValue(value).build();
			getStub(node).deleteBinding(b);
		} catch (Exception e) {
			error("delete() failed", e);
			throw new DhtBase.Failed("delete() RPC failed");
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

	public void deleteBinding(NodeInfo node, String key, String value) throws DhtBase.Failed {
		Log.weblog(TAG, "deleteBinding(" + key + ")");
		Binding binding = Binding.newBuilder().setKey(key).setValue(value).build();
		getStub(node).deleteBinding(binding);
	}


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

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		Log.weblog(TAG, "notify("+node.getId()+")");
		// TODO
//		try {
//			DhtServiceBlockingStub stub = getStub(node);
//			return stub.notify(predDb);
//		} catch (Exception e) {
//			error("Notify failed for node: " + node.getId(), e);
//			throw new DhtBase.Failed("Notify failed for node: " + node.getId());
//		}

		try{
			Log.weblog(TAG, "notify(" + node.getId()+")");
			return getStub(node).notify(predDb);
		}
		catch(Exception e)
		{
			error("notify() failed, try again", e);
			throw new DhtBase.Failed("notify() RPC failed, do better next time");
		}
		//throw new IllegalStateException("notify() not yet implemented");
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
	}

	/*
	 * Listening for new bindings.
	 */
	public void listenOn(NodeInfo node, Subscription subscription, IEventListener listener) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOn("+node.getId()+")");
		// TODO listen for updates for the key specified in the subscription
		try
		{
			DhtServiceStub stub=getListenerStub(node);
			EventConsumer eventConsumer = EventConsumer.create(subscription.getKey(), listener);
			Subscription newSub = Subscription.newBuilder()
					.setKey(subscription.getKey())
					.setId(subscription.getId())
					.build();
			stub.listenOn(newSub, eventConsumer);
		}
		catch (Exception e) {
			error("Failed to listenOn for key: " + subscription.getKey(), e);
			throw new DhtBase.Failed("Failed to listenOn for key: " + subscription.getKey());
		}
	}

	public void listenOff(NodeInfo node, Subscription subscription) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOff("+node.getId()+")");
		// TODO stop listening for updates on bindings to the key in the subscription
		try {
			DhtServiceBlockingStub stub = getStub(node);
			stub.listenOff(subscription);
		} catch (Exception e) {
			error("Failed to listenOff for key: " + subscription.getKey(), e);
			throw new DhtBase.Failed("Failed to listenOff for key: " + subscription.getKey());
		}
	}


}
