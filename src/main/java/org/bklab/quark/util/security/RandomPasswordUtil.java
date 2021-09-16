package org.bklab.quark.util.security;

import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomPasswordUtil {
    private static final char[] LOWER_CHARACTERS = new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
    private static final char[] UPPER_CHARACTERS = new char[]{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
    private static final char[] DIGITAL_CHARACTERS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static final char[] SPECIALLY_CHARACTERS = new char[]{'!', '#', '$', '%', '&', '*', '+', '-', '.', '?', '^', '~'};
    private final Random random = new Random();
    private int maxSpecialCount;
    private Predicate<String> validTester;

    public RandomPasswordUtil() {
    }

    public RandomPasswordUtil(int maxSpecialCount) {
        this.maxSpecialCount = maxSpecialCount;
    }

    public String random(int lowerCount, int upperCount, int digitalCount, int specialCount) {
        List<Integer> list = IntStream.range(0, lowerCount).mapToObj(a -> 0).collect(Collectors.toList());
        IntStream.range(0, upperCount).forEach(i -> list.add(random.nextInt(list.size()), 1));
        IntStream.range(0, digitalCount).forEach(i -> list.add(random.nextInt(list.size()), 2));
        IntStream.range(0, specialCount).forEach(i -> list.add(random.nextInt(list.size()), 3));
        String password = createPassword(list);
        return validTester != null && !validTester.test(password) ? random(lowerCount, upperCount, digitalCount, specialCount) : password;
    }

    public String random(int length) {
        List<Integer> list = IntStream.range(0, length - maxSpecialCount).mapToObj(a -> random.nextInt(3)).collect(Collectors.toList());
        IntStream.range(0, maxSpecialCount).forEach(i -> list.add(random.nextInt(list.size()), 3));
        String password = createPassword(list);
        return validTester != null && !validTester.test(password) ? random(length) : password;
    }

    private String createPassword(List<Integer> list) {
        StringBuilder password = new StringBuilder();
        for (Integer integer : list) {
            switch (integer) {
                case 1:
                    password.append(UPPER_CHARACTERS[random.nextInt(UPPER_CHARACTERS.length)]);
                    break;
                case 2:
                    password.append(DIGITAL_CHARACTERS[random.nextInt(DIGITAL_CHARACTERS.length)]);
                    break;
                case 3:
                    password.append(SPECIALLY_CHARACTERS[random.nextInt(SPECIALLY_CHARACTERS.length)]);
                    break;
                default:
                    password.append(LOWER_CHARACTERS[random.nextInt(LOWER_CHARACTERS.length)]);
                    break;
            }
        }
        return password.toString();
    }

    public RandomPasswordUtil setValidTester(Predicate<String> validTester) {
        this.validTester = validTester;
        return this;
    }

    public RandomPasswordUtil setMaxSpecialCount(int maxSpecialCount) {
        this.maxSpecialCount = maxSpecialCount;
        return this;
    }
}
