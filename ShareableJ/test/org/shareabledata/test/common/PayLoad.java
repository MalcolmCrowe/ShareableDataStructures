package org.shareabledata.test.common;

public class PayLoad  implements Comparable<PayLoad>, Cloneable {
	private String payload;
	
	public PayLoad(String payload){
		this.payload = payload;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((payload == null) ? 0 : payload.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		PayLoad other = (PayLoad) obj;
		if (payload == null) {
			if (other.payload != null) {
				return false;
			}
		} else if (!payload.equals(other.payload)) {
			return false;
		}
		return true;
	}

	@Override
	public int compareTo(PayLoad arg0) {
		// TODO Auto-generated method stub
		return this.payload.compareTo(arg0.getPayload());
	}
        
        @Override
        public Object clone(){
            return new PayLoad(this.getPayload());
        }
	
	
}	
