package net.thumbtack.sharding.test.common;

import java.io.Serializable;
import java.util.Date;

public class Entity implements Serializable {
    public long id;
    public String text;
    public Date date;

    public Entity() {}

    public Entity(long id, String text, Date date) {
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

        Entity Entity = (Entity) o;

        return id == Entity.id &&
                !(date != null ? !date.equals(Entity.date) : Entity.date != null) &&
                !(text != null ? !text.equals(Entity.text) : Entity.text != null);

    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (text != null ? text.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Entity{" +
                "id=" + id +
                ", text='" + text + '\'' +
                ", date=" + date +
                '}';
    }
}
