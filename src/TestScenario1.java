/**
 * Created by dianli on 5/26/16.
 */
class TestScenario1 {

    public static void main(String[] argv) throws Exception {
        User user1 = new User("Euronext Paris");
        user1.register("1", "1", "investor");
        user1.login("1", "1");
        User user2 = new User("Euronext Paris");
        user2.register("2", "2", "investor");
        user2.login("2", "2");
        Runner r1 = new Runner(user1);
        r1.start();
        Runner r2 = new Runner(user2);
        r2.start();
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
