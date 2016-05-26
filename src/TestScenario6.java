/**
 * Created by dianli on 5/26/16.
 */
public class TestScenario6 {
    public static void main(String[] argv) throws Exception {
        for (int i = 0; i < 1000; i++) {
            User user = new User("Euronext Paris");
            user.register(Integer.toString(i), Integer.toString(i), "investor");
            user.login(Integer.toString(i), Integer.toString(i));
            Runner r = new Runner(user);
            r.start();
        }
    }

    static class Runner implements Runnable {
        private Thread t;
        private User user;

        Runner(User user) {
            this.user = user;
        }

        @Override
        public void run() {
            try {
                user.summary();
                for (int i = 0; i < 5; i++)
                    user.buyStock("nokia", 1);
                for (int i = 0; i < 5; i++)
                    user.sellStock("nokia", 1);
                user.summary();
                user.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void start() {
            t = new Thread(this);
            t.start();
        }
    }
}
