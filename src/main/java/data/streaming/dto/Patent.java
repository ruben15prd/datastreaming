package data.streaming.dto;

public class Patent {

	private String title;
	private String date;
	private String idPatent;

	public Patent(String title, String date, String idPatent) {
		super();
		this.title = title;
		this.date = date;
		this.idPatent = idPatent;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getIdPatent() {
		return idPatent;
	}

	public void setIdPatent(String idPatent) {
		this.idPatent = idPatent;
	}

	@Override
	public String toString() {
		return "Patent [title=" + title + ", date=" + date + ", idPatent=" + idPatent + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((idPatent == null) ? 0 : idPatent.hashCode());
		result = prime * result + ((title == null) ? 0 : title.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Patent other = (Patent) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (idPatent == null) {
			if (other.idPatent != null)
				return false;
		} else if (!idPatent.equals(other.idPatent))
			return false;
		if (title == null) {
			if (other.title != null)
				return false;
		} else if (!title.equals(other.title))
			return false;
		return true;
	}

}
