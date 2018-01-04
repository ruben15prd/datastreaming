package data.streaming.dto;

import java.util.List;

public class PatentFull implements Comparable<PatentFull>{

	private String title;
	private String date;
	private String idPatent;
	private List<String> inventors;
	private List<String> keywords;
	
	public PatentFull(String title, String date, String idPatent, List<String> inventors, List<String> keywords) {
		super();
		this.title = title;
		this.date = date;
		this.idPatent = idPatent;
		this.inventors = inventors;
		this.keywords = keywords;
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

	public List<String> getInventors() {
		return inventors;
	}

	public void setInventors(List<String> inventors) {
		this.inventors = inventors;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	@Override
	public String toString() {
		return "PatentFull [title=" + title + ", date=" + date + ", idPatent=" + idPatent + ", inventors=" + inventors
				+ ", keywords=" + keywords + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((idPatent == null) ? 0 : idPatent.hashCode());
		result = prime * result + ((inventors == null) ? 0 : inventors.hashCode());
		result = prime * result + ((keywords == null) ? 0 : keywords.hashCode());
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
		PatentFull other = (PatentFull) obj;
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
		if (inventors == null) {
			if (other.inventors != null)
				return false;
		} else if (!inventors.equals(other.inventors))
			return false;
		if (keywords == null) {
			if (other.keywords != null)
				return false;
		} else if (!keywords.equals(other.keywords))
			return false;
		if (title == null) {
			if (other.title != null)
				return false;
		} else if (!title.equals(other.title))
			return false;
		return true;
	}

	@Override
	public int compareTo(PatentFull ptf) {
		int res = this.getIdPatent().compareTo(ptf.getIdPatent());
		return res;
	}
	
	
	



}
