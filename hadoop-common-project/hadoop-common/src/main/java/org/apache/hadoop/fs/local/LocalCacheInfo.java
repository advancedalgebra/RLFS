package org.apache.hadoop.fs.local;

import org.apache.commons.lang.builder.EqualsBuilder;

import java.util.Date;

public class LocalCacheInfo {
    private String tag = null;
    private final long expiryTime;

    public LocalCacheInfo() {
        this.tag = "invalid";
        this.expiryTime = -1L;
    }

    public LocalCacheInfo(String tag) {
        this.tag = tag;
        this.expiryTime = new Date().getTime();;
    }

    public String getTag() {
        return tag;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    public void setTag(String tag) {
        this.tag = (tag == null) ? "default" : tag;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{");
        sb.append("tag=" + tag);
        sb.append("; expiryTime=" + expiryTime);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        LocalCacheInfo other = (LocalCacheInfo) o;
        return new EqualsBuilder().append(getTag(), other.getTag()).
                append(getExpiryTime(), other.getExpiryTime()).
                isEquals();
    }
}
