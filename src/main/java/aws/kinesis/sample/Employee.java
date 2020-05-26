package aws.kinesis.sample;

public class Employee {
	
	public Employee(String name, String designation, int id) {
		super();
		this.name = name;
		this.designation = designation;
		this.id = id;
	}

	private String name;
	
	private String designation;
	
	private int id;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesignation() {
		return designation;
	}

	public void setDesignation(String designation) {
		this.designation = designation;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	
}
