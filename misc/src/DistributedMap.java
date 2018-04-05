import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistributedMap implements SimpleStringMap {

    private final HashMap<String, String> distributedHashMap;
    JChannel channel;

    public DistributedMap() throws Exception {
        this.distributedHashMap = new HashMap<>();
        start();
        channel.getState(null, 0);
    }

    @Override
    public boolean containsKey(String key) {
        return distributedHashMap.containsKey(key);
    }

    @Override
    public String get(String key) {
        return distributedHashMap.get(key);
    }

    @Override
    public String put(String key, String value) throws Exception {
        String v = distributedHashMap.put(key, value);

        Message msg = new Message(null, null, this.distributedHashMap);
        channel.send(msg);

        return v;
    }

    @Override
    public String remove(String key) throws Exception {
        String v = distributedHashMap.remove(key);

        Message msg = new Message(null, null, this.distributedHashMap);
        channel.send(msg);

        return v;
    }


    void start() throws Exception {

        channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.68")))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
//                .addProtocol(new SEQUENCER())
//                .addProtocol(new FLUSH())
                .addProtocol(new STATE_TRANSFER());
        stack.init();

        channel.setReceiver(new ReceiverAdapter() {
            @Override
            public void viewAccepted(View view) {
                //super.viewAccepted(view);
                //System.out.println(view.toString());
                handleView(channel, view);
            }

            @Override
            public void receive(Message msg) {
                //System.out.println("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBb");
                Map<String, String> hashmap;

                hashmap = (Map<String, String>) msg.getObject();

                synchronized (distributedHashMap) {
                    distributedHashMap.clear();
                    distributedHashMap.putAll(hashmap);
                }

            }

            @Override
            public void getState(OutputStream output) throws Exception {
                synchronized (distributedHashMap) {
                    Util.objectToStream(distributedHashMap, new DataOutputStream(output));
                }
            }

            @Override
            public void setState(InputStream input) throws Exception {
                //System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa");
                Map<String, String> hashmap;

                hashmap = (Map<String, String>) Util.objectFromStream(new DataInputStream(input));

                synchronized (distributedHashMap) {
                    distributedHashMap.clear();
                    distributedHashMap.putAll(hashmap);
                }
            }
        });

        channel.connect("hashmap");
    }

    private static void handleView(JChannel ch, View new_view) {
        if(new_view instanceof MergeView) {
            ViewHandler handler=new ViewHandler(ch, (MergeView)new_view);
            // requires separate thread as we don't want to block JGroups
            handler.start();
        }
    }


    private static class ViewHandler extends Thread {
        JChannel ch;
        MergeView view;

        private ViewHandler(JChannel ch, MergeView view) {
            this.ch=ch;
            this.view=view;
        }

        public void run() {
            List<View> subgroups=view.getSubgroups();
            View tmp_view=subgroups.get(0); // picks the first
            Address local_addr=ch.getAddress();
            if(!tmp_view.getMembers().contains(local_addr)) {
                System.out.println("Not member of the new primary partition ("
                        + tmp_view + "), will re-acquire the state");
                try {
                    ch.getState(tmp_view.getMembers().get(0), 30000);
                } catch(Exception ex) { }
            }
            else {
                System.out.println("Not member of the new primary partition ("
                        + tmp_view + "), will do nothing");
            }
        }
    }

    public HashMap<String, String> getDistributedHashMap() {
        return distributedHashMap;
    }
}
