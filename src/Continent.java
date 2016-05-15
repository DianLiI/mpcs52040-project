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
    private Map<String, AbsAddress> stockTable; // key: stock, value: address of market
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
    public AbsAddress localStockLookUp(String stockName) throws Exception {
        // check self
        if (stockTable.containsKey(stockName)) {
            return this.stockTable.get(stockName);
        }
        // ask this continent
        MethodCall hasStock = new MethodCall("hasStock", new Object[]{}, new Class[]{});
        RspList<Boolean> cRspList = Util.callOthersMethods(this.cDisp, hasStock, this.cDispOptions);
        for (Map.Entry<Address, Rsp<Boolean>> entry: cRspList.entrySet()) {
            if (entry.getValue() == null)
                continue;
            if (entry.getValue().equals(true)) {
                AbsAddress absAddress = new AbsAddress(this.name, entry.getKey());
                this.stockTable.put(stockName, absAddress);
                return absAddress;
            }
        }
        return null;
    }

    @SuppressWarnings("unused")
    public AbsAddress marketLookUp(String stockName) throws Exception {

        AbsAddress absAddress = this.localStockLookUp(stockName);
        if (absAddress != null)
            return absAddress;
        // ask other continents
        MethodCall localStockLookUp = new MethodCall("localStockLookUp", new Object[]{stockName}, new Class[]{String.class});
        RspList<AbsAddress> rRspList = Util.callOthersMethods(this.rDisp, localStockLookUp, this.rDispOptions);
        absAddress = rRspList.getFirst();
        if (absAddress != null) {
            this.stockTable.put(stockName, absAddress);
        }
        return absAddress;
    }

    public void syncTime(Date date) throws Exception {
        MethodCall syncTime = new MethodCall("syncTime", new Object[]{date}, new Class[]{Date.class});
        Util.callOthersMethods(this.cDisp, syncTime, this.cDispOptions);
    }
}
