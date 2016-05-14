/**
 * Created by dianli on 5/14/16.
 */
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

public class Continent {
    private String name;
    private Map<String, Address> stockTable; // key: stock, value: address of market
    private JChannel rChannel; // root channel, cluster of continents
    private JChannel cChannel; // continent channel, cluster of markets
    private RpcDispatcher rDisp; // root dispatcher, listening on root channel
    private RpcDispatcher cDisp; // continent dispatcher listening on continent channel
    private RequestOptions rDispOptions = new RequestOptions(ResponseMode.GET_ALL, 1000);
    private RequestOptions cDispOptions = new RequestOptions(ResponseMode.GET_ALL, 5000);

    public Continent(String name) throws Exception {
        this.name = name;
        this.rChannel = new JChannel();
        this.cChannel = new JChannel();
        this.rChannel.connect("Root");
        this.cChannel.connect(name);
        this.rDisp = new RpcDispatcher(this.rChannel, this);
        this.cDisp = new RpcDispatcher(this.cChannel, this);
        this.stockTable = new Hashtable<>();
    }

    @SuppressWarnings("unused")
    public Address localMarketLookUp(String stockName) throws Exception {
        // check self
        if (stockTable.containsKey(stockName)) {
            return stockTable.get(stockName);
        }
        // ask this continent
        MethodCall hasStock = new MethodCall("hasStock", new Object[]{}, new Class[]{});
        RspList<Boolean> cRspList = Util.callOthersMethods(this.cDisp, hasStock, this.cDispOptions);
        for (Map.Entry<Address, Rsp<Boolean>> entry: cRspList.entrySet()) {
            if (entry.getValue() == null)
                continue;
            if (entry.getValue().equals(true)) {
                Address addr = entry.getKey();
                this.stockTable.put(stockName, addr);
                return addr;
            }
        }
        return null;
    }

    @SuppressWarnings("unused")
    public Address marketLookUp(String stockName) throws Exception {

        Address addr = this.localMarketLookUp(stockName);
        if (addr != null)
            return addr;
        // ask other continents
        MethodCall localMarketLookUp = new MethodCall("localMarketLookUp", new Object[]{stockName}, new Class[]{String.class});
        RspList<Address> rRspList = Util.callOthersMethods(this.rDisp, localMarketLookUp, this.rDispOptions);
        addr = rRspList.getFirst();
        if (addr != null) {
            this.stockTable.put(stockName, addr);
        }
        return addr;
    }

    public void syncTime(Date date) throws Exception {
        MethodCall syncTime = new MethodCall("syncTime", new Object[]{date}, new Class[]{Date.class});
        Util.callOthersMethods(this.cDisp, syncTime, this.cDispOptions);
    }
}
