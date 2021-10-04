import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class t {
  public List<Integer> getNumbersUsingIntStreamIterate(int start, int limit) {
    return IntStream.iterate(start, i -> i + 1)
      .limit(limit)
      .boxed()
      .collect(Collectors.toList());
  }

  public List<Double> getNumbersUsingFloatStreamIterate(int start, int limit) {
    return DoubleStream.iterate(start, i -> i + 1)
      .limit(limit)
      .boxed()
      .collect(Collectors.toList());
  }


  public static void main(String[] args) {
    t a = new t();
    System.out.println(a.getNumbersUsingIntStreamIterate(0,8192));
    System.out.println(a.getNumbersUsingFloatStreamIterate(0,8192));
  }
}
