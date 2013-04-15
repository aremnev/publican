package net.thumbtack.sharding;

import java.util.Date;

public class CommonEntity {
    public long id;
    public String text;
    public Date date;

    public CommonEntity() {}

    public CommonEntity(long id, String text, Date date) {
        this.id = id;
        this.text = text;
        this.date = date;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommonEntity entity = (CommonEntity) o;

        return id == entity.id &&
                !(date != null ? !date.equals(entity.date) : entity.date != null) &&
                !(text != null ? !text.equals(entity.text) : entity.text != null);

    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        return result;
    }
}
