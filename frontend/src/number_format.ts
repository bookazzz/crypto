export function decimal(value:number,maximumFractionDigits=8,minimumFractionDigits=0){
  return new Intl.NumberFormat('en-US',{
    useGrouping:false,
    maximumFractionDigits,
    minimumFractionDigits,
  }).format(value);
}
