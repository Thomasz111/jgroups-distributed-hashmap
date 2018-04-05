import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main
{
    public static void main(String[] args) throws Exception{
        System.setProperty("java.net.preferIPv4Stack", "true");
        DistributedMap map = new DistributedMap();
        Scanner in = new Scanner(System.in);

        while(true){
            System.out.println("Me wezwanie:");
            List<String> command = Arrays.asList(in.nextLine().split(" "));
            switch(command.get(0)){
                case "remove":
                    map.remove(command.get(1));
                    break;
                case "put":
                    map.put(command.get(1), command.get(2));
                    break;
                case "contains":
                    System.out.println(map.containsKey(command.get(1)));
                    break;
                case "print":
                    System.out.println(map.getDistributedHashMap());
                    break;
            }
        }
    }
}
