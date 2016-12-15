package org.simpleflatmapper.csv.parser;

public final class TextFormat {

    public static final TextFormat CSV = new TextFormat(',', '"', false, false, true);
    public static final TextFormat TSV = new TextFormat('\t', '"', false, false, true);

    public final char separatorChar;
    public final char escapeChar;
    public final boolean yamlComment;
    public final boolean trimSpaces;
    public final boolean unescape;

    public TextFormat(char separatorChar, char escapeChar, boolean yamlComment, boolean trimSpaces, boolean unescape) {
        this.separatorChar = separatorChar;
        this.escapeChar = escapeChar;
        this.yamlComment = yamlComment;
        this.trimSpaces = trimSpaces;
        this.unescape = unescape;
    }

    public TextFormat withSeparatorChar(char separatorChar) {
        return new TextFormat(separatorChar, escapeChar, yamlComment, trimSpaces, unescape);
    }

    public TextFormat withEscapeChar(char escapeChar) {
        return new TextFormat(separatorChar, escapeChar, yamlComment, trimSpaces, unescape);
    }


    public TextFormat withYamlComment(boolean yamlComment) {
        return new TextFormat(separatorChar, escapeChar, yamlComment, trimSpaces, unescape);
    }

    public TextFormat withTrimSpaces(boolean trimSpaces) {
        return new TextFormat(separatorChar, escapeChar, yamlComment, trimSpaces, unescape);
    }

    public TextFormat withUnescape(boolean unescape) {
        return new TextFormat(separatorChar, escapeChar, yamlComment, trimSpaces, unescape);
    }

}
