
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
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
    private RpcDispatcher cDisp; // continent dispatcher
    private RequestOptions cDispOptions;
    private RpcDispatcher mDisp; // market dispatcher
    private UserTable userTable;
    private StockTicker stockTicker;

    public Market(String name, String currency, String continent) throws Exception {
        this.continent = continent;
        this.name = name;
        this.currency = currency;
        this.cChannel = new JChannel();
        this.mChannel = new JChannel();
        this.cChannel.connect(this.continent);
        this.cDisp = new RpcDispatcher(this.cChannel, this);
        this.cDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);
        this.mDisp = new RpcDispatcher(this.mChannel, this);
        String db_name = "db_" + this.name.toLowerCase();
        this.stockTicker = new StockTicker(db_name);
        this.userTable = new UserTable(db_name);
    }

    @SuppressWarnings("unused")
    public Return register(String username, String password, String type) {
        return new Return(null, this.userTable.register(username, password, type));
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
        MethodCall localStockLookUp = new MethodCall("stockLookUp", new Object[]{stockname}, new Class[]{String.class});
        RspList<AbsAddress> rRspList = Util.callOthersMethods(this.cDisp, localStockLookUp, this.cDispOptions);
        return rRspList.getFirst();
    }
    @SuppressWarnings("unused")
    public Return sellStock(Investor investor, String stockname, int amount) {
        try {
            int left = investor.stockTable.get(stockname);
            if (left >= amount) {
                double price = this.stockTicker.sellStocks(stockname, amount);
                investor.stockTable.put(stockname, left - amount);
                investor.balance += price * amount;
                this.userTable.updateUser(investor);
                String msg = String.format("Filled: Sell %s %d %f", stockname, amount, price);
                System.out.println(msg);
                return new Return(msg, true, investor);
            }
            else if (left > 0){
                String msg = String.format("Not enough Inventory: Sell %s %d > %d", stockname, amount, left);
                System.out.println(msg);
                return new Return(msg, false);
            }
            else {
                System.out.println("Redirect");
                return new Return("Redirect", false, this.stockLookUp(stockname));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            String msg = String.format("Not enough Inventory: Sell %s %d > %d", stockname, amount, 0);
            System.out.println(msg);
            return new Return(msg, false);
        }
        return new Return("Error", false);
    }

    public Return buyStock(Investor investor, String stockname, int amount) {
        try {
            int left = this.stockTicker.hasStock(stockname);
            if (left > amount) {
                double price = this.stockTicker.getPrice(stockname);
                if (price * amount <= investor.balance) {
                    this.stockTicker.buyStocks(stockname, amount);
                    if (!investor.stockTable.containsKey(stockname)) {
                        investor.stockTable.put(stockname, 0);
                    }
                    investor.stockTable.put(stockname, investor.stockTable.get(stockname) + amount);
                    investor.balance -= price * amount;
                    this.userTable.updateUser(investor);
                    String msg = String.format("Filled: Sell %s %d %f", stockname, amount, price);
                    System.out.println(msg);
                    return new Return(msg, true, investor);
                }
                else {
                    String msg = "Not enough balance";
                    System.out.println(msg);
                    return new Return(msg, false);
                }
            }
            else if (left > 0){
                String msg = String.format("Not enough Inventory: buy %s %d > %d", stockname, amount, left);
                System.out.println(msg);
                return new Return(msg, false);
            }
            else {
                System.out.println("Redirect");
                return new Return("Redirect", false, this.stockLookUp(stockname));
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
            m = new Market("Euronext_Paris", "EURO", "Europe");
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
//            m.register("haha", "hehe", "investor");
//            Investor investor = (Investor) m.login("haha", "hehe");
//            investor = (Investor) m.buyStock(investor, "ACCOR", 10).object;
//            m.sellStock(investor, "ACCOR", 20);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
