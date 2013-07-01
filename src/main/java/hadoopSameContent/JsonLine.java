package hadoopSameContent;


public class JsonLine {
    private boolean foundEventType = false;
    private final String json;
    private final String origin;
    private String vdna_widget_mc;
    
    public JsonLine(String json, String origin){
        this.origin = origin;
        this.json = json;
    }
    
    public String getVdna_widget_mc() {
        return vdna_widget_mc;
    }

    public void setVdna_widget_mc(String vdna_widget_mc) {
        this.vdna_widget_mc = vdna_widget_mc;
    }

    public void setFoundEventType(){
        this.foundEventType = true;
    }
    
    public boolean hasFoundEventType(){
        return foundEventType;
    }

    public String getIntermediateOutput(){
        return origin + "\t" + json;
    }
}
