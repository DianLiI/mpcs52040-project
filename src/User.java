import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.cs.Client;
import org.jgroups.util.RspList;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by dianli on 5/14/16.
 */
public class User {
    String username;
    JChannel pChannel; // permanent channel, connected to registered market
    JChannel tChannel; // temporary channel, for cross-market exchanges
    RpcDispatcher pDisp;
    RequestOptions pDispOptions;
    RpcDispatcher tDisp;
    Investor investor;
    public static void main(String argv[]) throws Exception {
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        User user = new User();
        while(true) {
            String line=in.readLine().trim().toLowerCase();
            String[] lst = line.split(" ");
            if (lst[0].equals("register")) {
                user.register(lst[1], lst[2], "investor");
                user.investor = user.login(lst[1], lst[2]);
                user.username = lst[1];
            }else if (lst[0].equals("sell")) {
                user.sellStock(lst[1], Integer.parseInt(lst[2]));
            }else if (lst[0].equals("buy")) {
                user.buyStock(lst[1], Integer.parseInt(lst[2]));
            }else if (lst[0].equals("summary")){
                user.summary();
            }
        }
    }

    public User() throws Exception {
        this.pChannel = new JChannel();
        this.pChannel.connect("Europe");
        this.pDisp = new RpcDispatcher(this.pChannel, this);
        this.pDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
    }

    private void summary() {
        System.out.println("Client: " + this.username);
        System.out.println("=========== STOCK LISTED============");
        for (String k : this.investor.stockTable.keySet()) {
            System.out.println(k + " " + this.investor.stockTable.get(k));
        }
        System.out.println("=========== CASH FLOW===============");
        System.out.println(this.investor.balance);
    }
    private void register(String username, String password, String type) throws Exception {
        MethodCall register = new MethodCall("register", new Object[]{username, password, type},
                new Class[]{String.class, String.class, String.class});
        Util.callOthersMethods(this.pDisp, register, this.pDispOptions);
    }

    private Investor login(String username, String password) throws Exception {
        MethodCall login = new MethodCall("login", new Object[]{username, password},
                new Class[]{String.class, String.class});
        RspList<Investor> rspList = Util.callOthersMethods(this.pDisp, login, this.pDispOptions);
        return rspList.getFirst();
    }


    private void sellStock(String stockName, int amount) throws Exception {
        MethodCall sellStock = new MethodCall("sellStock", new Object[]{this.investor, stockName, amount},
                new Class[]{Investor.class, String.class, int.class});
        RspList<Return> rspList = Util.callOthersMethods(this.pDisp, sellStock, this.pDispOptions);
        Return ret = rspList.getFirst();
        if (ret.success)
            this.investor = (Investor) ret.object;
    }

    private void buyStock(String stockName, int amount) throws Exception {
        MethodCall buyStock = new MethodCall("buyStock", new Object[]{this.investor, stockName, amount},
                new Class[]{Investor.class, String.class, int.class});
        RspList<Return> rspList = Util.callOthersMethods(this.pDisp, buyStock, this.pDispOptions);
        Return ret = rspList.getFirst();
        if (ret.success)
            this.investor = (Investor) ret.object;
    }
}

