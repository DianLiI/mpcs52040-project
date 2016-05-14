import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.util.List;

/**
 * Created by dianli on 5/14/16.
 */
final class Util {
    private Util() {}; // can't initiate this static class

    static RspList callOthersMethods(RpcDispatcher disp, MethodCall methodCall, RequestOptions options) throws Exception {
        Channel channel = disp.getChannel();
        List<Address> addrs = channel.getView().getMembers();
        addrs.remove(channel.getAddress());
        return disp.callRemoteMethods(addrs, methodCall, options);
    }
}
