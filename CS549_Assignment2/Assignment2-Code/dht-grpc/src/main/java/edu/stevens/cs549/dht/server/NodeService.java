package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceImplBase;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService extends DhtServiceImplBase {
	
	private static final String TAG = NodeService.class.getCanonicalName();
	
	private static Logger logger = Logger.getLogger(TAG);

	/**
	 * Each service request is processed by a distinct service object.
	 *
	 * Shared state is in the state object; we use the singleton pattern to make sure it is shared.
	 */
	private Dht getDht() {
		return Dht.getDht();
	}
	
	// TODO: add the missing operations

	@Override
	public void getPred(Empty req, StreamObserver<OptNodeInfo> responseObserver) {
		Log.weblog(TAG, "getPred()");
		try {
			OptNodeInfo pred = getDht().getPred();
			responseObserver.onNext(pred);
		} catch (Exception e) {
			responseObserver.onNext(OptNodeInfo.getDefaultInstance());
		}
		responseObserver.onCompleted();
	}

	@Override
	public void getSucc(Empty req, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getSucc()");
		try {
			NodeInfo succ = getDht().getSucc();
			if (succ == null) {
				error("getSucc() returned null!", new Exception());
				succ = NodeInfo.getDefaultInstance(); // Prevents client-side crash
			}
			responseObserver.onNext(succ);
		} catch (Exception e) {
			error("getSucc() failed", e);
			responseObserver.onNext(NodeInfo.getDefaultInstance());
		}
		responseObserver.onCompleted();
	}



	@Override
	public void closestPrecedingFinger(Id req, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "closestPrecedingFinger(" + req.getId() + ")");
		try {
			NodeInfo finger = getDht().closestPrecedingFinger(req.getId());
			if (finger == null) {
				error("closestPrecedingFinger() returned null!", new Exception());
				finger = NodeInfo.getDefaultInstance();
			}
			responseObserver.onNext(finger);
		} catch (Exception e) {
			error("closestPrecedingFinger failed", e);
			responseObserver.onNext(NodeInfo.getDefaultInstance());
		}
		responseObserver.onCompleted();
	}


	@Override
	public void findSuccessor(Id req, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "findSuccessor(" + req.getId() + ")");
		try {
			NodeInfo result = getDht().findSuccessor(req.getId());
			if (result == null) {
				error("findSuccessor() returned null!", new Exception());
				result = NodeInfo.getDefaultInstance();
			}
			responseObserver.onNext(result);
		} catch (Exception e) {
			error("findSuccessor failed", e);
			responseObserver.onNext(NodeInfo.getDefaultInstance());
		}
		responseObserver.onCompleted();
	}


	@Override
	public void notify(NodeBindings req, StreamObserver<OptNodeBindings> responseObserver) {
		Log.weblog(TAG, "notify()");
		try {
			OptNodeBindings result = getDht().notify(req);
			responseObserver.onNext(result);
		} catch (Exception e) {
			error("notify() failed", e);
			responseObserver.onNext(OptNodeBindings.getDefaultInstance());
		}
		responseObserver.onCompleted();
	}

	public void getBindings(Key req, StreamObserver<Bindings> responseObserver) {
		Log.weblog(TAG, "get(" + req.getKey() + ")");
		try {
			String[] values = getDht().get(req.getKey());  // Use existing method
			Bindings.Builder builder = Bindings.newBuilder();
			for (String value : values) {
				builder.addValue(value);
			}
			responseObserver.onNext(builder.build());
		} catch (Invalid e) {
			error("getBindings() failed", e);
			responseObserver.onNext(Bindings.getDefaultInstance());
		}
		responseObserver.onCompleted();
	}

	@Override
	public void addBinding(Binding req, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "add(" + req.getKey() + ", " + req.getValue() + ")");
		try {
			getDht().add(req.getKey(), req.getValue());
		} catch (Invalid e) {
			error("addBinding() failed", e);
		}
		responseObserver.onNext(Empty.getDefaultInstance());
		responseObserver.onCompleted();
	}

	@Override
	public void deleteBinding(Binding req, StreamObserver<Empty> responseObserver) {
		Log.weblog(TAG, "delete(" + req.getKey() + ", " + req.getValue() + ")");
		try {
			getDht().delete(req.getKey(), req.getValue());
		} catch (Invalid e) {
			error("deleteBinding() failed", e);
		}
		responseObserver.onNext(Empty.getDefaultInstance());
		responseObserver.onCompleted();
	}


	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

	@Override
	public void getNodeInfo(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getNodeInfo()");
		responseObserver.onNext(getDht().getNodeInfo());
		responseObserver.onCompleted();
	}


}