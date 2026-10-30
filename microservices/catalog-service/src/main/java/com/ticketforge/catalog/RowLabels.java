package com.ticketforge.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ticketforge.shared.ApiException;

/** Expands "A-Z", "1-50", "A,B,C" or "A" into row labels. */
final class RowLabels {

    private RowLabels() {
    }

    static List<String> expand(String spec) {
        String s = spec.trim();
        if (s.contains(",")) {
            return Arrays.stream(s.split(",")).map(String::trim).filter(p -> !p.isEmpty()).toList();
        }
        int dash = s.indexOf('-');
        if (dash < 0) {
            return List.of(s);
        }
        String from = s.substring(0, dash).trim();
        String to = s.substring(dash + 1).trim();
        List<String> out = new ArrayList<>();
        if (from.chars().allMatch(Character::isDigit) && to.chars().allMatch(Character::isDigit)
                && !from.isEmpty() && !to.isEmpty()) {
            int a = Integer.parseInt(from);
            int b = Integer.parseInt(to);
            if (a > b || b - a > 500) {
                throw invalid(spec);
            }
            for (int i = a; i <= b; i++) {
                out.add(Integer.toString(i));
            }
            return out;
        }
        if (from.length() == 1 && to.length() == 1 && Character.isLetter(from.charAt(0))
                && Character.isLetter(to.charAt(0)) && from.charAt(0) <= to.charAt(0)) {
            for (char c = from.charAt(0); c <= to.charAt(0); c++) {
                out.add(String.valueOf(c));
            }
            return out;
        }
        throw invalid(spec);
    }

    private static ApiException invalid(String spec) {
        return ApiException.badRequest("INVALID_ROW_LABELS", "Cannot parse row labels '" + spec + "'");
    }
}
