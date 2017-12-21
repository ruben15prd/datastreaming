package data.streaming.dto;

import java.util.List;

public class Recomendation {
	private String patent;
	private List<String> recomendations;
	
	
	public Recomendation(String patent, List<String> recomendations) {
		super();
		this.patent = patent;
		this.recomendations = recomendations;
	}
	
	public String getPatent() {
		return patent;
	}
	public void setPatent(String patent) {
		this.patent = patent;
	}
	public List<String> getRecomendations() {
		return recomendations;
	}
	public void setRecomendations(List<String> recomendations) {
		this.recomendations = recomendations;
	}

	@Override
	public String toString() {
		return "Recomendation [patent=" + patent + ", recomendations=" + recomendations + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((patent == null) ? 0 : patent.hashCode());
		result = prime * result + ((recomendations == null) ? 0 : recomendations.hashCode());
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
		Recomendation other = (Recomendation) obj;
		if (patent == null) {
			if (other.patent != null)
				return false;
		} else if (!patent.equals(other.patent))
			return false;
		if (recomendations == null) {
			if (other.recomendations != null)
				return false;
		} else if (!recomendations.equals(other.recomendations))
			return false;
		return true;
	}
	
	
	
	
}
