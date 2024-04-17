import java.io.IOException;
import java.util.ArrayList;

/** * @author Wael Abouelsaadat */

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;


	// Default constructor
	public SQLTerm(){

	}

	public SQLTerm(  String _strTableName,String _strColumnName, String _strOperator, Object _objValue ){
		this._strTableName = _strTableName;
		this._strColumnName = _strColumnName;
		this._strOperator = _strOperator;
		this._objValue = _objValue;
	}

	public boolean invalidOperator(){
		if(!_strOperator.equals("=") && !_strOperator.equals(">") && !_strOperator.equals("<") && !_strOperator.equals("!=") && !_strOperator.equals(">=")&& !_strOperator.equals("<="))
			return true;
		return false;
	}

	public ArrayList<Tuple> searchEqualToWithIndex(BPTree ind, String pagename, int indexInPage) throws IOException, ClassNotFoundException {
		ArrayList<Tuple> tuples = new ArrayList<>();
		ArrayList<Ref> references = ind.search((Comparable) this._objValue);
		pagename = references.get(0).getPage();
		Page pagenow = Page.deserialize(pagename );
		for(int i=0; i< references.size();i++){
			indexInPage=references.get(i).getIndexInPage();
			tuples.add(pagenow.tuples.get(indexInPage));
		}
		return tuples;
	}


}