import java.util.*;

/**
 * implement a (main-memory) data store with MVTO. objects are <int, int>
 * key-value pairs. if an operation is to be refused by the MVTO protocol, undo
 * its xact (what work does this take?) and throw an exception. garbage
 * collection of versions is not required. Throw exceptions when necessary, such
 * as when we try to execute an operation in a transaction that is not running;
 * when we insert an object with an existing key; when we try to read or write a
 * nonexisting key, etc. Keep the interface, we want to test automatically!
 *
 **/

public class MVTO {

	private static Map<Integer, ArrayList<DbObject>> db = new HashMap<Integer, ArrayList<DbObject>>();
	private static Map<Integer, Xact> xactions = new HashMap<Integer, Xact>();
	private static Map<Integer, HashSet<Integer>> dependencies = new HashMap<Integer, HashSet<Integer>>();
	private static int max_xact = 0;

	// returns transaction id == logical start timestamp
	public static int begin_transaction() {
		max_xact += 1;
		// put xact to the map of xactions
		xactions.put(max_xact, new Xact(max_xact));
		// initialize dependency list of xact to an empty list
		dependencies.put(max_xact, new HashSet<Integer>());
		return max_xact;
	}

	// create and initialize new object in transaction xact
	public static void insert(int xact, int key, int value) throws Exception {
		System.out.println("T(" + xact + "):" + "I(" + key + "," + value + ")");
		// If object already exists, throw an exception
		if (db.containsKey(key)) {
			rollback(xact);
			throw new Exception("KEY ALREADY EXISTS IN T(" + xact + "):I(" + key + ")");

		} else {
			// Create a new object whose wts and rts are xact and put it in the db
			db.put(key, new ArrayList<DbObject>());
			db.get(key).add(new DbObject(value, xact, xact));
			// Write the action to the log of xact
			xactions.get(xact).writeToLog(new Action(key, null, null, null, value, xact, xact));
		}

	}

	// return value of object key in transaction xact
	public static int read(int xact, int key) throws Exception {
		// If object doesn't exists, throw an exception
		if (!db.containsKey(key)) {
			rollback(xact);
			throw new Exception("KEY DOESN'T EXISTS IN T(" + xact + "):R(" + key + ")");
		} else {
			// Find the object with largest ts s.t it's smaller than xact
			ArrayList<DbObject> objects = db.get(key);
			int maxTS = 0;
			int index = -1;
			for (int i = 0; i < objects.size(); i++) {
				if (objects.get(i).wts <= xact && objects.get(i).wts > maxTS) {
					maxTS = objects.get(i).wts;
					index = i;
				}
			}
			if (index == -1) {
				rollback(xact);
				throw new Exception("T(" + xact + "):KEY DOESN'T SATISFY ANY READ CONDITION");
			}
			// if xact > rts, update rts
			else if (xact > objects.get(index).rts) {
				// Write the action to the log of xact
				xactions.get(xact).writeToLog(new Action(key, objects.get(index).content, objects.get(index).wts,
						objects.get(index).rts, objects.get(index).content, objects.get(index).wts, xact));
				db.get(key).get(index).rts = xact;

			}
			// xact reads something written by another transaction (identified by wts).
			// If that other transaction still exists (not rolledback or commited), make xact depend on it
			if (xact != objects.get(index).wts) {
				if (xactions.containsKey(objects.get(index).wts)) {
					dependencies.get(xact).add(objects.get(index).wts);
				}
			}
			System.out.println("T(" + xact + "):" + "R(" + key + ")" + " => " + objects.get(index).content);
			return objects.get(index).content;
		}
	}

	// write value of existing object identified by key in transaction xact
	public static void write(int xact, int key, int value) throws Exception {
		System.out.println("T(" + xact + "):" + "W(" + key + "," + value + ")");
		if (!db.containsKey(key)) {
			rollback(xact);
			throw new Exception("KEY DOESN'T EXISTS IN T(" + xact + "):W(" + key + "," + value + ")");
		} else {
			// find the object with largest ts s.t it's smaller than xact
			ArrayList<DbObject> objects = db.get(key);
			int maxTS = 0;
			int index = -1;
			for (int i = 0; i < objects.size(); i++) {
				if (objects.get(i).wts <= xact && objects.get(i).wts > maxTS) {
					maxTS = objects.get(i).wts;
					index = i;
				}
			}
			if (index == -1) {
				rollback(xact);
				throw new Exception("T(" + xact + "):KEY DOESN'T SATISFY ANY WRITE CONDITION");
			} else if (xact < objects.get(index).rts) {
				rollback(xact);
				throw new Exception("ROLLBACK " + "T" + "(" + xact + "):" + "W(" + key + "," + value + ")");
			} else if (xact >= objects.get(index).rts) {
				// create a new version of the object
				if (xact > objects.get(index).wts) {
					db.get(key).add(new DbObject(value, xact, xact));
					// write the action to the log of xact
					xactions.get(xact).writeToLog(new Action(key, null, 
							null, null, value, xact, xact));
				}
				// update content
				else if (xact == objects.get(index).wts) {
					// write the action to the log of xact
					xactions.get(xact).writeToLog(new Action(key, objects.get(index).content, objects.get(index).wts,
							objects.get(index).rts, value, objects.get(index).wts, objects.get(index).rts));
					db.get(key).get(index).content = value;		
				}

			}
		}
	}

	public static void commit(int xact) throws Exception {
		System.out.println("T" + "(" + xact + "):" + "COMMIT START");
		if (!xactions.containsKey(xact)) {
			throw new Exception("T" + "(" + xact + "):" + "DOES NOT EXIST");
		} else {
			// set the flag of xact and attempt to commit it
			xactions.get(xact).hasCommitReq = true;
			attemptToCommit(xact);
		}
	}

	// check if we can commit xact
	public static void attemptToCommit(Integer xact) {
		// check if xact has any dependencies and commit request
		if (xactions.get(xact).hasCommitReq && dependencies.get(xact).isEmpty()) {
			// print commit message and remove xact from the map of transactions
			System.out.println("T" + "(" + xact + "):" + "COMMIT FINISH");
			xactions.remove(xact);
			// remove xact from everyones dependencies and attempt to commit
			// everything that depends on it recursively
			for (Integer key : dependencies.keySet()) {
				if (dependencies.get(key).contains(xact)) {
					dependencies.get(key).remove(xact);
					attemptToCommit(xactions.get(key).id);
				}
			}

		}
	}

	public static void rollback(int xact) throws Exception {
		if (!xactions.containsKey(xact)) {
			throw new Exception("T" + "(" + xact + "):" + "DOES NOT EXIST");
		}
		if (xactions.get(xact).hasCommitReq) {
			System.out.println("T" + "(" + xact + "):" + "COMMIT UNSUCCESSFUL");
		}
		System.out.println("T" + "(" + xact + "):" + "ROLLBACK");
		// rollback everything that depends on xact
		for (Integer key : dependencies.keySet()) {
			if (dependencies.get(key).contains(xact)) {
				dependencies.get(key).remove(xact);
				rollback(xactions.get(key).id);
			}
		}
		// undo everything done by xact and then remove it from the map of transactions
		Collections.reverse(xactions.get(xact).log);
		for (Action a : xactions.get(xact).log) {
			Iterator<DbObject> iter = db.get(a.objectKey).iterator();
			while(iter.hasNext()) {
				DbObject o = iter.next();
				if (o.content == a.newContent && o.wts == a.newWts && o.rts == a.newRts) {
					// if this object is initialized by xact, remove it altogether
					if (a.prevContent == null) {
						iter.remove();
					}
					// else, simply undo the action of xact on this object
					else {
						o.content = a.prevContent;
						o.rts = a.prevRts;
						o.wts = a.prevWts;
					}
				}
			}
		}
		xactions.remove(xact);
	}
}

class DbObject {
	int content;
	int wts;
	int rts;

	public DbObject(int content, int wts, int rts) {
		this.content = content;
		this.wts = wts;
		this.rts = rts;
	}
	
	@Override
    public String toString() {
        return "Content:" + content + " WTS: " + wts +
        		" RTS: " + rts;
	}
}

class Xact {
	Integer id;
	Boolean hasCommitReq;
	List<Action> log;
	
	public Xact(int id) {
		this.id = id;
		this.hasCommitReq = false;
		this.log = new ArrayList<Action>();

	}

	//just a convenience function to update log
	public void writeToLog(Action action) {
		log.add(action);
	}

}

class Action {
	Integer objectKey;
	// previous values can be null thus object
	Integer prevContent;
	Integer prevWts;
	Integer prevRts;
	int newContent;
	int newWts;
	int newRts;

	public Action(Integer objectKey, Integer prevContent, Integer prevWts, Integer prevRts, int newContent, int newWts,
			int newRts) {
		this.objectKey = objectKey;
		this.prevContent = prevContent;
		this.prevWts = prevWts;
		this.prevRts = prevRts;
		this.newContent = newContent;
		this.newWts = newWts;
		this.newRts = newRts;
	}
	
	@Override
    public String toString() {
        return "Key:" + objectKey + " PrevContent:" + prevContent +
        		" PrevWts:" + prevWts + " PrevRts:" + prevRts +
        		" NewContent:" + newContent + " newWts:" + newWts +
        		" newRts:" + newRts;

    }

}