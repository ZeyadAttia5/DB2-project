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

	public boolean validOperator(){
		if(!_strOperator.equals("=") && !_strOperator.equals(">") && !_strOperator.equals("<") && !_strOperator.equals("!=") && !_strOperator.equals(">=")&& !_strOperator.equals("<="))
			return false;
		return true;
	}

}