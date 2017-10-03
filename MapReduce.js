
function Map (var key1, var val1, var context)
{
	var ligne;

	ligne=val1.split(",");

	var map2;

	for(var col_num=0, col_num < ligne.length(), col_num++)
	{
		map2[col_num][0] = col_num;
		map2[col_num][1] = ligne[col_num];
	}
	context.set(key1, map2);
}

function reduce (var key2, var val2, var context)
{
	var val3;

	for(key2=0; key2<val2.length(), key2++)
	{
		val3[val2[key2][0]=val2[key2][1];
	}
	context.set(key2, val3);


}
