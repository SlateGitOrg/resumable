
const n=(value,fallback=0)=>Number.isFinite(Number(value))?Number(value):fallback;
const clamp=(value,low,high)=>Math.min(high,Math.max(low,value));
const round=(value,digits=2)=>Number(value.toFixed(digits));
const mean=values=>values.length?values.reduce((sum,value)=>sum+value,0)/values.length:0;
const parseJSON=(value,fallback=[])=>{try{return JSON.parse(value)}catch{return fallback}};
const valuesFrom=value=>String(value).split(/[\s,]+/).map(Number).filter(Number.isFinite);
const result=(status,summary,metrics,rows,detail='')=>({status,summary,metrics,rows,detail});
const erf=x=>{const sign=x<0?-1:1,a=Math.abs(x),t=1/(1+0.3275911*a);const y=1-(((((1.061405429*t-1.453152027)*t)+1.421413741)*t-0.284496736)*t+0.254829592)*t*Math.exp(-a*a);return sign*y};
const normalCdf=z=>0.5*(1+erf(z/Math.sqrt(2)));
const wilson=(successes,total)=>{if(!total)return[0,0];const z=1.96,p=successes/total,d=1+z*z/total,c=(p+z*z/(2*total))/d,h=z*Math.sqrt((p*(1-p)+z*z/(4*total))/total)/d;return[clamp(c-h,0,1),clamp(c+h,0,1)]};
const sha256=async value=>{const bytes=new TextEncoder().encode(String(value));const digest=await crypto.subtle.digest('SHA-256',bytes);return[...new Uint8Array(digest)].map(byte=>byte.toString(16).padStart(2,'0')).join('')};
const tag=(xml,name)=>xml.match(new RegExp('<'+name+'[^>]*>([\\s\\S]*?)<\\/'+name+'>','i'))?.[1]?.trim()??'';
const similarity=(a,b)=>{const x=String(a).toLowerCase(),y=String(b).toLowerCase();if(x===y)return 1;const A=new Set(x.split(/\W+/).filter(Boolean)),B=new Set(y.split(/\W+/).filter(Boolean));const inter=[...A].filter(v=>B.has(v)).length;return inter/Math.max(1,new Set([...A,...B]).size)};

export const meta={"slug":"resumable","name":"Resumable","eyebrow":"Event-stream recovery","description":"Resolve a Last-Event-ID reconnect against the available replay buffer and expose gaps explicitly.","fields":[{"name":"lastSeen","label":"Last event ID seen by client","type":"number","min":0,"max":1000000000,"step":1,"help":""},{"name":"earliestBuffered","label":"Earliest buffered event ID","type":"number","min":0,"max":1000000000,"step":1,"help":""},{"name":"newestBuffered","label":"Newest buffered event ID","type":"number","min":0,"max":1000000000,"step":1,"help":""},{"name":"maxReplay","label":"Maximum replay events","type":"number","min":1,"max":100000,"step":1,"help":""}]};
export const initialState={"lastSeen":10420,"earliestBuffered":10300,"newestBuffered":10482,"maxReplay":500};
export const alternateState={"lastSeen":9800,"earliestBuffered":10300,"newestBuffered":10482,"maxReplay":500};
export async function compute(i){const last=n(i.lastSeen),earliest=n(i.earliestBuffered),newest=n(i.newestBuffered),max=n(i.maxReplay),missing=Math.max(0,newest-last),gap=last<earliest-1,tooLarge=missing>max,status=gap?'Gap detected':tooLarge?'Replay limit exceeded':'Replay available',start=Math.max(last+1,earliest),count=gap?0:Math.min(missing,max);return result(status,gap?`Client requested ${last}, but the buffer begins at ${earliest}; full resync is required.`:`Replay ${count} events from ${start} through ${newest}.`,[{label:'Missing events',value:missing},{label:'Replay count',value:count},{label:'Buffer floor',value:earliest},{label:'Continuity',value:gap?'Broken':'Proven'}],[{step:'Client watermark',eventId:last},{step:'Buffer earliest',eventId:earliest},{step:'Replay start',eventId:gap?'Not safe':start},{step:'Newest event',eventId:newest}], 'A reconnect never silently skips beyond the retained buffer.')}
