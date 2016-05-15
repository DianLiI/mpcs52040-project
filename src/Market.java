import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;
public class Market {
    private String continent;
    private String name;
    private String currency;
    private JChannel cChannel; // continent channel, used for address resolving
    private JChannel mChannel; // market channel, used for connection the market
    private RpcDispatcher cDisp; // continent dispatcher
    private RpcDispatcher mDisp; // market dispatcher

    public Market(String name, String currency, String continent) throws Exception {
        this.continent = continent;
        this.name = name;
        this.currency = currency;
        this.cChannel = new JChannel();
        this.mChannel = new JChannel();
        this.cChannel.connect(this.continent);
        this.cDisp = new RpcDispatcher(this.cChannel, this);
        this.mDisp = new RpcDispatcher(this.mChannel, this);
    }
    @SuppressWarnings("unused")
    public Return register(String username, String password) {
        return new Return(null, true);
    }

    @SuppressWarnings("unused")
    public Return login(String username, String password) {
        return new Return(null, true);
    }

    @SuppressWarnings("unused")
    public Return issueStock(Company company, int amount) {
        return new Return(null, true);
    }

    @SuppressWarnings("unused")
    public Return sellStock(Investor investor, int amount) {
        return new Return(null, true);
    }

    @SuppressWarnings("unused")
    public Return buyStock(Investor investor, int amount) {
        return new Return(null, true);
    }

    @SuppressWarnings("unused")
    public boolean hasStock(String stockName) {
        return false;
    }
}
