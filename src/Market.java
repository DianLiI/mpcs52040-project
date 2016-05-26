
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.UDP;
import org.jgroups.util.RspList;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Market {
    private String continent;
    private String name;
    private String currency;
    private JChannel cChannel; // continent channel, used for address resolving
    private JChannel mChannel; // market channel, used for connection the market
    private JChannel tChannel; // temporary channel.
    private JChannel lChannel; // local channel, used for consensus control
    private RpcDispatcher cDisp; // continent dispatcher
    private RequestOptions cDispOptions;
    private RpcDispatcher mDisp; // market dispatcher
    private RequestOptions mDispOptions;
    private RpcDispatcher tDisp;
    private RequestOptions tDispOptions;
    private RpcDispatcher lDisp;
    private RequestOptions lDispOptions;
    private UserTable userTable;
    private StockTicker stockTicker;

    public Market(String name, String currency, String continent) throws Exception {
        this.continent = continent;
        this.name = name;
        this.currency = currency;
        this.cChannel = new JChannel();
        this.mChannel = new JChannel();
        this.tChannel = new JChannel();
        this.lChannel = new JChannel();
        this.cChannel.setDiscardOwnMessages(true);
        this.mChannel.setDiscardOwnMessages(true);
        this.tChannel.setDiscardOwnMessages(true);
        this.lChannel.setDiscardOwnMessages(true);
        this.cChannel.getProtocolStack().findProtocol(UDP.class).setValue("mcast_port", 45590);
        this.cChannel.connect(this.continent);
        this.cDisp = new RpcDispatcher(this.cChannel, this);
        this.cDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
        ;
        this.mChannel.getProtocolStack().findProtocol(UDP.class).setValue("mcast_port", 45589);
        this.mChannel.connect(this.name.split("_")[0]);
        this.mDisp = new RpcDispatcher(this.mChannel, this);
        this.mDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
        this.tDisp = new RpcDispatcher(this.tChannel, this);
        this.tDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
        this.tChannel.getProtocolStack().findProtocol(UDP.class).setValue("mcast_port", 45590);
        this.lChannel.getProtocolStack().findProtocol(UDP.class).setValue("mcast_port", 45587);
        this.lChannel.connect(this.name.split("_")[0] + "_local");
        this.lDisp = new RpcDispatcher(this.lChannel, this);
        this.lDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
        String db_name = this.name;
        this.isLeader();
        this.stockTicker = new StockTicker(db_name, this.isLeader());
        this.userTable = new UserTable(db_name);
    }

    private boolean isLeader() {
        return this.lChannel.getView().getMembers().get(0).equals(this.lChannel.getAddress());
    }

    @SuppressWarnings("unused")
    public Return register(String username, String password, String type) {
        boolean success = this.userTable.register(username, password, type);
        if (success) {
            return new Return(null, success);
        }
        return new Return("Account already exisits", false);
    }

    @SuppressWarnings("unused")
    public Holder login(String username, String password) throws SQLException {
        return this.userTable.login(username, password);
    }

    @SuppressWarnings("unused")
    public Return issueStock(Company company, int amount) {
        return new Return(null, true);
    }

    @SuppressWarnings("unused")
    public boolean hasStock(String stockname) {
        try {
            return this.stockTicker.hasStock(stockname) > 0;
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private AbsAddress stockLookUp(String stockname) throws Exception {
        MethodCall stockLookUp = new MethodCall("stockLookUp", new Object[]{stockname}, new Class[]{String.class});
        RspList<AbsAddress> rRspList = this.cDisp.callRemoteMethods(null, stockLookUp, this.cDispOptions);
        return rRspList.getFirst();
    }
    @SuppressWarnings("unused")
    public Return sellStock(Investor investor, String stockname, int amount) {
        if (investor == null) {
            return new Return("Please login", false);
        }
        try {
            int left = investor.stockTable.get(stockname);
            if (left >= amount) {
                if (!this.isLeader())
                    return null;
                double price = this.stockTicker.sellStocks(investor.username, stockname, amount);
                investor.stockTable.put(stockname, left - amount);
                investor.balance += price * amount;
                this.userTable.updateUser(investor);
                String msg = String.format("Filled: Sell %s %d %f", stockname, amount, price);
                System.out.println(msg);
                return new Return(msg, true, investor);
            }
            else if (left > 0){
                if (!this.isLeader())
                    return null;
                String msg = String.format("Not enough Inventory: Sell %s %d > %d", stockname, amount, left);
                System.out.println(msg);
                return new Return(msg, false);
            }
            else {
                if (!this.isLeader())
                    return null;
                System.out.println("Redirect");
//                return new Return("Redirect", false, this.stockLookUp(stockname));
                AbsAddress remoteAddress = this.stockLookUp(stockname);
                if (remoteAddress == null)
                    return null;
                this.tChannel.connect(remoteAddress.clusterName, remoteAddress.address, 5000);
                MethodCall sellStock = new MethodCall("sellStock",
                        new Object[]{investor, stockname, amount}, new Class[]{Investor.class, String.class, int.class});
                Return ret = this.tDisp.callRemoteMethod(remoteAddress.address, sellStock, this.tDispOptions);
                if (ret == null) {
                    return new Return("Aborted", false);
                }
                if (ret.success) {
                    this.userTable.updateUser((Investor) ret.object);
                }
                this.tChannel.close();
                return ret;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            if (!this.isLeader())
                return null;
            String msg = String.format("Not enough Inventory: Sell %s %d > %d", stockname, amount, 0);
            System.out.println(msg);
            return new Return(msg, false);
        }
        return new Return("Error", false);
    }

    public Return buyStock(Investor investor, String stockname, int amount) {
        if (investor == null) {
            return new Return("Please login", false);
        }
        try {
            int left = this.stockTicker.hasStock(stockname);
            if (left > amount) {
                double price = this.stockTicker.getPrice(stockname);
                if (price * amount <= investor.balance) {
                    if (!this.isLeader())
                        return null;
                    Return ret = this.stockTicker.buyStocks(investor.username, stockname, amount);
                    if (!ret.success)
                        return ret;
                    if (!investor.stockTable.containsKey(stockname)) {
                        investor.stockTable.put(stockname, 0);
                    }
                    investor.stockTable.put(stockname, investor.stockTable.get(stockname) + amount);
                    price = (Double) ret.object;
                    investor.balance -= price * amount;
                    this.userTable.updateUser(investor);
                    String msg = String.format("Filled: Buy %s %d %f", stockname, amount, price);
                    System.out.println(msg);
                    return new Return(msg, true, investor);
                }
                else {
                    if (!this.isLeader())
                        return null;
                    String msg = "Not enough balance";
                    System.out.println(msg);
                    return new Return(msg, false);
                }
            }
            else if (left > 0){
                if (!this.isLeader())
                    return null;
                String msg = String.format("Not enough Inventory: buy %s %d > %d", stockname, amount, left);
                System.out.println(msg);
                return new Return(msg, false);
            }
            else {
                if (!this.isLeader())
                    return null;
                System.out.println("Redirect");
                AbsAddress remoteAddress = this.stockLookUp(stockname);
                if (remoteAddress == null)
                    return null;
                this.tChannel.connect(remoteAddress.clusterName, remoteAddress.address, 5000);
                MethodCall buyStock = new MethodCall("buyStock",
                        new Object[]{investor, stockname, amount}, new Class[]{Investor.class, String.class, int.class});
                Return ret = this.tDisp.callRemoteMethod(remoteAddress.address, buyStock, this.tDispOptions);
                if (ret == null) {
                    return new Return("Aborted", false);
                }
                if (ret.success) {
                    this.userTable.updateUser((Investor) ret.object);
                }
                this.tChannel.close();
                return ret;
//                return new Return("Redirect", false, this.stockLookUp(stockname));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Return("Error", false);
    }

    public static void main(String[] argv){
        Market m = null;
        try {
            m = new Market(argv[0], argv[1], argv[2]);
            BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
            while(true) {
                String line=in.readLine().trim().toLowerCase();
                if (line.equals("next hour")) {
                    m.stockTicker.time_offset += 3600000;
                    SimpleDateFormat ft = new SimpleDateFormat();
                    java.util.Date time = new java.util.Date();
                    System.out.println(ft.format(time.getTime() + m.stockTicker.time_offset));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
