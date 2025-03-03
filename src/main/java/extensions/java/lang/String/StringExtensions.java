package extensions.java.lang.String;

import manifold.ext.rt.api.Extension;
import manifold.ext.rt.api.This;

@Extension
public class StringExtensions {

    @Extension
    public static String addExclamationMark(@This String str) {
        return str + "!";
    }
}
