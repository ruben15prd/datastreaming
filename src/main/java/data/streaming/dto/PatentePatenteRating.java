package data.streaming.dto;

import org.bson.Document;

public class PatentePatenteRating {
	private String patent1;
	private String patent2;
	private Integer rating;
	
	public PatentePatenteRating(String idPatente1, String idPatente2, Integer rating) {
		super();
		this.patent1 = idPatente1;
		this.patent2 = idPatente2;
		this.rating = rating;
	}

	public String getPatent1() {
		return patent1;
	}

	public void setPatent1(String patent1) {
		this.patent1 = patent1;
	}

	public String getPatent2() {
		return patent2;
	}

	public void setPatent2(String patent2) {
		this.patent2 = patent2;
	}

	public Integer getRating() {
		return rating;
	}

	public void setRating(Integer rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "PatentePatenteRating [patent1=" + patent1 + ", patent2=" + patent2 + ", rating=" + rating + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((patent1 == null) ? 0 : patent1.hashCode());
		result = prime * result + ((patent2 == null) ? 0 : patent2.hashCode());
		result = prime * result + ((rating == null) ? 0 : rating.hashCode());
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
		PatentePatenteRating other = (PatentePatenteRating) obj;
		if (patent1 == null) {
			if (other.patent1 != null)
				return false;
		} else if (!patent1.equals(other.patent1))
			return false;
		if (patent2 == null) {
			if (other.patent2 != null)
				return false;
		} else if (!patent2.equals(other.patent2))
			return false;
		if (rating == null) {
			if (other.rating != null)
				return false;
		} else if (!rating.equals(other.rating))
			return false;
		return true;
	}
	
	
}
