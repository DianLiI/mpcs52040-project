/**
 * Created by dianli on 5/26/16.
 */
public class TestScenario2 {
    public static void main(String argv[]) throws Exception {
        User user = new User("Euronext Paris");
        user.register("1", "1", "investor");
        user.login("1", "1");
        for (int i = 0; i < 10; i++) {
            user.buyStock("nokia", 1);
            Thread.sleep(1000);
        }
        user.summary();
        user.close();
    }
}
