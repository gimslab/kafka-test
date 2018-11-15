package kafkatest;

public class Sleeper {

	public static void sleep(long i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void sleepSec(int s) {
		for (int i = 0; i < s; i++) {
			System.out.print((i + 1) + " ");
			sleep(1000);
		}
	}
}
