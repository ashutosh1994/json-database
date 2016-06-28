var file = require('fs');
var path = require('path')

var mkdirSync = function(path){
	var parts=path.split("/");
	var temp="";
	
	for (i=1;i<parts.length;i++){
		temp+="/"+parts[i];
		try{
			file.mkdirSync(temp)
		}catch(e)
		{
			if(e.code!='EEXIST') throw e;
		}
	}
}

var _directory_table;
var method = JSON_Database.prototype;

function JSON_Database(){	
}

method.GetDatabase=function(database_name,_path){
	this._database_name= database_name;
	this._database_root="";
	if(path.resolve(_path)==path.normalize(_path)){
		this._database_root=_path+"/"+database_name;
	}else
	{
		this._database_root=path.resolve(_path)+"/"+database_name;
	}
	this._database_root=this._database_root.replace("\\","/")
	
	try{
	mkdirSync(this._database_root);
	}catch(e){
		console.log("Error in fetching Database:"+ e);
	}
	_directory_table=this._database_root+"/tables";
}

method.CreateDatabase=function(database_name,_path){
	this._database_name= database_name;
	this._database_root="";
	if(path.resolve(_path)==path.normalize(_path)){
		this._database_root=_path+"/"+database_name;
	}else
	{
		this._database_root=path.resolve(_path)+"/"+database_name;
	}
	this._database_root=this._database_root.replace("\\","/")
	
	try{
	mkdirSync(this._database_root);
	}catch(e){
		console.log("Error in creating Database:"+ e);
	}
	_directory_table=this._database_root+"/tables";
	
}

method.WriteToTable=function(table_name,JSONobject,fun){

	var file_dir=this._database_root+"/tables";
	var file_dir=_directory_table;
	var JSONObject=[];
	
	if(Array.isArray(JSONobject)){
		JSONObject=JSONobject;

	}
	else {
		JSONObject.push(JSONobject);
	}
	
	try{
		file.mkdirSync(file_dir)
	}catch(e){
		err="Error in creating the tables: ";
		
	}
	var filename=file_dir+"/"+table_name+".table";
    var error,status;
	try{
  				stats=file.statSync(filename);
  				if (stats.size!=0){
  					try{
  						for(i in JSONObject)
  					file.appendFile(filename,",\r\n"+JSON.stringify(JSONObject[i]));
  					
  				}
  				catch(e){
  					console.log("Error in writing the table")
  				}
  			
  				}else
  				throw "Error";

  				
  			}catch(e){
  				try{
  						for( i in JSONObject){
  							console.log(i);
  							if(i==0)
  							file.writeFile(filename,JSON.stringify(JSONObject[i]));
  							else
  							
  							file.appendFile(filename,",\r\n"+JSON.stringify(JSONObject[i]));

  					}
  						
  					
  				}
  				catch(e){
  					console.log("Error in creating the table")
  				}
  			}
  	fun(error,status);
}

function WriteToTable1(table_name,fun){
	
	var file_dir=_directory_table;
	
	try{
		file.mkdirSync(file_dir)
	}catch(e){
		err="Error in creating the tables: ";
		if(e.code!='EEXIST') throw err+"The database already exists";
	}
	var filename=file_dir+"/"+table_name+".table";
    var error,status;
	try{
  				stats=file.statSync(filename);
  				if (stats.size!=0){
  					try{
  					file.appendFileSync(filename,"");
  					
  				}
  				catch(e){
  					console.log("Error in writing to table"+e)
  				}
  				}else
  				throw "Error";

  				
  			}catch(e){
  				try{
  				file.writeFileSync(filename,"");
  					
  				}
  				catch(e){
  					console.log("Error in writing to table"+e)
  				}
  			}
  	fun(error,status);
}

method.CreateTable=function(table_name){

	WriteToTable1(table_name,function(err,status){
		if(!err){
			console.log("Successfully created the table.");
		}else{
			console.log(err)
		}
	})
}

method.DeleteTable=function(table_name,fun){
	var filename=this._database_root+"/tables"+"/"+table_name+".table";
	try{
	file.unlinkSync(filename);
		}
		catch(e){
			console.log("Error in deleting the table");
		}
		fun(err,success);
}

var ReadFromTable=function(filename,fun){
	var obj,error;
	try{
		var data=file.readFileSync(filename)
		obj = JSON.parse("["+data+"]");
	   }catch(e){
	   	error=e;
	   }
		fun(error,obj);
}

function Equals(Obj1,Obj2){
	
	if(Object.keys(Obj1).length!=Object.keys(Obj2).length)
		return false
	for (i in Obj1){
		if(!Obj2[i]) return false;
		if(Obj1[i]!=Obj2[i]) return false;
	}
	return true;
}

method.SelectFromTable=function(table_name,where_clauses,opts,fun){


	var Obj;
	ReadFromTable(_directory_table+"/"+table_name+".table",function(err,obj){
		if(!err){
			Obj=obj;
		}else
		{
			var Error="Could not find the table: "+table_name;
			throw Error;
		}
	})
	if(Array.isArray(where_clauses)){
		where_clauses=where_clauses;

	}
	else {
		temp=where_clauses
		where_clauses=[]
		where_clauses.push(temp);
	}
	var result=[];
	var error;
	var _all=0,_not=0,_like=0,_output;
	if(opts.like) _like=1;
	if(opts.not) _not=1;
	if(opts.all) _all=1;
	if(opts.output) _output=1;
	if(!where_clauses.length) _all=1;
	var indices=[]

	if(_all){
		for(i in Obj){
			indices.push(i)
		}
	
	}else{
		for (i in Obj){
			for (j in where_clauses){
				obj=Obj[i];
				where_clause=where_clauses[j];
				if(_like){
					

					var flag=0;
					var lim=(Object.keys(where_clause)).length;
					for(k in where_clause){
						
						if((obj[k].toString()).search(where_clause[k])+1){
							flag++;
						}
						
					}
					if(flag==lim){
						
						indices.push(i);
						break;
					}
					
				}
				
				else{
					var flag=0;
					var lim=(Object.keys(where_clause)).length;
					for(k in where_clause){
						
						if(where_clause[k]==obj[k]){
							flag++;
						}
						
					}
					if(flag==lim){
						
						indices.push(i);
						break;
					}
					
				}
			}
		}
	}

	if(_not){
		var new_indices=[]
		for ( i in Obj){
			if (!(i in indices)){
				new_indices.push(i)
			}
		}
		indices=new_indices
	}
	
	for (i in indices){
	
		result.push(Obj[indices[i]])
	}

	if(_output){

		var _out=opts.output;
		console.log(_out);
		var newResults=[]
		for(i in result){
			var temp="{ ";
			for (j=0;j<_out.length;j++){
				if(j==_out.length-1){
					temp+='"'+_out[j]+'"'+":"+result[i][_out[j]];
				}else
				{
					temp+='"'+_out[j]+'"'+":"+result[i][_out[j]]+",";
				}
			}
			temp+=" }";
			tempObj=JSON.parse(JSON.parse(JSON.stringify(temp)))
			
			newResults.push(tempObj);
		}
		result=newResults;
	}
		

	
	
	if(result==null){
		error="Could not find the data";
	}
	fun(error,result)
}

method.RemoveFromTable=function(table_name,where_clauses,opts,fun){
	var Obj;
	table=_directory_table+"/"+table_name+".table";
	
	
	ReadFromTable(table,function(err,obj){
		
		if(!err){
			Obj=obj;
		}else
		{
			var Error="Could not find the table: "+table_name;
			throw Error;
		}
	})
	if(Array.isArray(where_clauses)){
		where_clauses=where_clauses;

	}
	else {
		temp=where_clauses
		where_clauses=[]
		where_clauses.push(temp);
	}
	var old_Obj=Obj;
	var result=[];
	var error;
	
	var _all=0,_not=0,_like=0;
	if(opts.like) _like=1;
	if(opts.not) _not=1;
	if(opts.all) _all=1;
	if(!where_clauses.length) _all=1;
	var indices=[]

	if(_all){

		for (var i in Obj){
				indices.push(i)
			
		}
	
	}else{
		
		for (var i in Obj){
			obj = Obj[i]
			for (var j in where_clauses){
				var where_clause = where_clauses[j]
				if(_like){
					

					var flag=0;
					var lim=(Object.keys(where_clause)).length;
					for(k in where_clause){
						
						if((obj[k].toString()).search(where_clause[k])+1){
							flag++;
						}
						
					}
					if(flag==lim){
						
						indices.push(i);
						break;
					}
					
				}
				
				else{
					var flag=0;
					var lim=(Object.keys(where_clause)).length;
					for(k in where_clause){
						
						if(where_clause[k]==obj[k]){
							flag++;
						}
						
					}
					if(flag==lim){
						
						indices.push(i);
						break;
					}
					
				}
			}
		}
	
	}

	if(_not){
		var new_indices=[]
		for (i in Obj){
			if (!(i in indices))
				new_indices.push(i)
			
		}
		indices=new_indices
	}

	indices.sort();
		for(var j=indices.length-1;j>=0;j--){
			Obj.splice(indices[j],1);
		}
	console.log(Obj)
	result=Obj;
	if(result==null){
		error="Could not find the data";
	}
	var _status;
	console.log(result)
	var filename=this._database_root+"/tables"+"/"+table_name;
	try{
	file.unlinkSync(filename+".table")
	this.CreateTable(table_name)
	this.WriteToTable(table_name,result,function(err,status){
		
	})
	}catch(e)
	{
		console.log(e)
	}
	
	fun(error,_status)
}

method.UpdateToTable=function(table_name,where_clauses,update_clause,opts,fun){
	var Obj;
	table=_directory_table+"/"+table_name+".table";
	
	
	ReadFromTable(table,function(err,obj){
		
		if(!err){
			Obj=obj;
		}else
		{
			var Error="Could not find the table: "+table_name;
			throw Error;
		}
	})
	if(Array.isArray(where_clauses)){
		where_clauses=where_clauses;

	}
	else {
		temp=where_clauses
		where_clauses=[]
		where_clauses.push(temp);
	}
	var old_Obj=Obj;
	var result=[];
	var error;
	
	var _all=0,_not=0,_like=0;
	if(opts.like) _like=1;
	if(opts.not) _not=1;
	if(opts.all) _all=1;
	if(!where_clauses.length) _all=1;
	var indices=[]

	if(_all){

		for (var i in Obj){
				indices.push(i)
			
		}
	
	}else{
		
		for (var i in Obj){
			obj = Obj[i]
			for (var j in where_clauses){
				var where_clause = where_clauses[j]
				if(_like){
					

					var flag=0;
					var lim=(Object.keys(where_clause)).length;
					for(k in where_clause){
						
						if((obj[k].toString()).search(where_clause[k])+1){
							flag++;
						}
						
					}
					if(flag==lim){
						
						indices.push(i);
						break;
					}
					
				}
				
				else{
					var flag=0;
					var lim=(Object.keys(where_clause)).length;
					for(k in where_clause){
						
						if(where_clause[k]==obj[k]){
							flag++;
						}
						
					}
					if(flag==lim){
						
						indices.push(i);
						break;
					}
					
				}
			}
		}
	
	}

	if(_not){
		var new_indices=[]
		for (i in Obj){
			if (!(i in indices))
				new_indices.push(i)
			
		}
		indices=new_indices
	}
    
	
	for(i in indices){
		for (j in update_clause){
			Obj[indices[i]][j] = update_clause[j]
		}
	}

	result=Obj

	if(result==null){
		error="Could not find the data";
	}
	var _status;
	console.log(result)
	var filename=this._database_root+"/tables"+"/"+table_name;
	try{
	file.unlinkSync(filename+".table")
	this.CreateTable(table_name)
	this.WriteToTable(table_name,result,function(err,status){
		
	})
	}catch(e)
	{
		console.log(e)
	}
	
	fun(error,_status)
}
module.exports = JSON_Database;
/*SelectFromDB("hello",[{f:"giraffe",a:4}],{not:true,output:["a"]},function(err,result){
	console.log(result);
	
})*/

//mkdirSync("DNB/Koo")

