import com.sun.istack.internal.Nullable;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.util.RspList;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by dianli on 5/14/16.
 */
public class User {
    private String username;
    private JChannel pChannel; // permanent channel, connected to registered market
    private JChannel tChannel; // temporary channel, for cross-market exchanges
    private RpcDispatcher pDisp;
    private RequestOptions pDispOptions;
    private RpcDispatcher tDisp;
    private RequestOptions tDispOptions;
    private Investor investor;
    private boolean loggedin;
    public static void main(String argv[]) throws Exception {
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        User user = new User("Euronext Paris");
        while(true) {
            String line=in.readLine().trim().toLowerCase();
            String[] lst = line.split(",");
            if (lst[0].equals("register")) {
                if (user.register(lst[1], lst[2], "investor")) {
                    user.login(lst[1], lst[2]);
                }
            } else if (lst[0].equals("login")) {
                user.login(lst[1], lst[2]);
            } else if (user.loggedin) {
                if (lst[0].equals("sell")) {
                    user.sellStock(lst[1], Integer.parseInt(lst[2]));
                } else if (lst[0].equals("buy")) {
                    user.buyStock(lst[1], Integer.parseInt(lst[2]));
                } else if (lst[0].equals("summary")) {
                    user.summary();
                }
            } else {
                System.out.println("Please log in or register");
            }
        }
    }

    public User(String marketName) throws Exception {
        ProtocolStackConfigurator configs = ConfiguratorFactory.getStackConfigurator("udp.xml");
        this.pChannel = new JChannel(configs);
        this.pChannel.getProtocolStack().findProtocol(UDP.class).setValue("mcast_port", 45589);
        this.pChannel.setDiscardOwnMessages(true);
        this.pChannel.connect(marketName);
        this.tChannel = new JChannel();
        this.tChannel.getProtocolStack().findProtocol(UDP.class).setValue("mcast_port", 45590);
        this.tChannel.setDiscardOwnMessages(true);
        this.pDisp = new RpcDispatcher(this.pChannel, this);
        this.pDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
        this.tDisp = new RpcDispatcher(this.tChannel, this);
        this.tDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
        this.loggedin = false;
    }

    void summary() {
        System.out.println("Client: " + this.username);
        System.out.println("=========== STOCK LISTED============");
        for (String k : this.investor.stockTable.keySet()) {
            System.out.println(k + " " + this.investor.stockTable.get(k));
        }
        System.out.println("=========== CASH FLOW===============");
        System.out.println(this.investor.balance);
    }

    boolean register(String username, String password, String type) throws Exception {
        MethodCall register = new MethodCall("register", new Object[]{username, password, type},
                new Class[]{String.class, String.class, String.class});
        Return ret = (Return) this.pDisp.callRemoteMethods(null, register, this.pDispOptions).getFirst();
        if (ret == null) {
            System.out.println("Server not reachable");
            return false;
        }
        if (ret.success) {
            return true;
        }
        System.out.println(ret.message);
        return false;
    }

    void login(String username, String password) throws Exception {
        MethodCall login = new MethodCall("login", new Object[]{username, password},
                new Class[]{String.class, String.class});
        Investor investor = (Investor) this.pDisp.callRemoteMethods(null, login, this.pDispOptions).getFirst();
        if (investor != null) {
            this.investor = investor;
            this.username = username;
            this.loggedin = true;
            System.out.println("Logged in successfully");
            return;
        }
        System.out.println("Login failed");
        return;
    }

    void sellStock(String stockName, int amount) throws Exception {
        MethodCall sellStock = new MethodCall("sellStock", new Object[]{this.investor, stockName, amount},
                new Class[]{Investor.class, String.class, int.class});
        // First try local market
        RspList<Return> rspList = this.pDisp.callRemoteMethods(null, sellStock, this.pDispOptions);
        Return ret = rspList.getFirst();
        if (ret == null || ret.object == null) {
            System.out.println("Stock " + stockName + " not found!");
            return;
        }
        if (ret.success) {
            System.out.println(ret.message);
            this.investor = (Investor) ret.object;
            return;
        }
        // If found on other markets, sell the stocks there
        AbsAddress remoteAddress = (AbsAddress) ret.object;
        this.tChannel.connect(remoteAddress.clusterName, remoteAddress.address, 5000);
        this.tDisp.callRemoteMethod(remoteAddress.address, sellStock, this.tDispOptions);
    }

    void buyStock(String stockName, int amount) throws Exception {
        MethodCall buyStock = new MethodCall("buyStock", new Object[]{this.investor, stockName, amount},
                new Class[]{Investor.class, String.class, int.class});
        RspList<Return> rspList = this.pDisp.callRemoteMethods(null, buyStock, this.pDispOptions);
        Return ret = rspList.getFirst();
        if (ret == null) {
            System.out.println("Stock not found or aborted by server.");
            return;
        }
        if (ret.object == null) {
            System.out.println(ret.message);
            return;
        }
        if (ret.success) {
            System.out.println(ret.message);
            this.investor = (Investor) ret.object;
            return;
        }
        // If found on other markets, buy the stocks there
//        AbsAddress remoteAddress = (AbsAddress) ret.object;
//        this.tChannel.connect(remoteAddress.clusterName, remoteAddress.address, 5000);
//        System.out.println("connected");
//        this.tDisp.callRemoteMethod(remoteAddress.address, buyStock, this.tDispOptions);
    }

    public void close() {
        this.pChannel.close();
    }
}

