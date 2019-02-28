public class PalindromeUtils {

    public static boolean isPalindrome(String line) {
        line = line.replaceAll("\\s", "").toLowerCase();
        return line.equals(new StringBuilder(line).reverse().toString());
    }

}
