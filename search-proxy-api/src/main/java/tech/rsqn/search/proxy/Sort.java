package tech.rsqn.search.proxy;

public class Sort {
    
    public enum SortDirection {ASC, DSC};
    
    private String sortField;
    private SortDirection sortDirection;    
    
    public Sort(String sortField, SortDirection sortDirection) {
        super();
        this.sortField = sortField;
        this.sortDirection = sortDirection;
    }
    
    public String getSortField() {
        return sortField;
    }
    
    public void setSortField(String sortField) {
        this.sortField = sortField;
    }
    
    public SortDirection getSortDirection() {
        return sortDirection;
    }
    
    public void setSortDirection(SortDirection sortDirection) {
        this.sortDirection = sortDirection;
    }
}
