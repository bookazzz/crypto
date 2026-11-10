import React,{useEffect,useRef,useState} from 'react';
import {createRoot} from 'react-dom/client';
import {Chart} from './Chart';
import type {Snapshot,Signal,Setup,ScalpPlan,Trade,Report,PortfolioReport,ResearchReport,AccountFeed,AccountExecution,AccountReview,DecisionEnvelope,DecisionContext,DataQuality,EntryPlan,BookMetrics,SpotFuturesContext,MarketBackground,State,ForecastOutcomesReport,ForecastStatistics} from './types';
import {decimal} from './number_format';
import './style.css';
import './decision.css';

const money=(v:number|undefined)=>v===undefined?'—':decimal(v,2,2);
const price=(v:number|null|undefined)=>v==null?'—':decimal(v,8);
const date=(v:number)=>new Date(v).toLocaleString('ru-RU',{timeZone:'Europe/Moscow',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'});
const age=(v:number|undefined)=>!v?'нет данных':`${Math.max(0,Math.floor((Date.now()-v)/1000))}с назад`;
type SignalScope='current'|'history'|'all';
const TIMEFRAMES=['5','15','60','240'];
const tfName=(v:string)=>({'5':'5м','15':'15м','60':'1ч','240':'4ч'}[v]||v);
function signalScopeAt(signal:Signal,now:number):Exclude<SignalScope,'all'>{
  const delivery=signal.filter_results?.delivery;
  const historical=delivery?.mode==='history'||(delivery?.mode==null&&delivery?.recovered===true);
  const terminal=['late','scenario_invalidated','target_reached'].includes(signal.entry_assessment?.status??'');
  const event=signal.event_at??signal.time,interval=Number(signal.timeframe)*60_000;
  return !historical&&!terminal&&interval>0&&now>=event&&now-event<=interval?'current':'history';
}
const statusName:Record<string,string>={found:'Найден',pending_retest:'Ожидает ретест',rejected:'Отклонен',open:'Позиция открыта',closed_sl:'Закрыт по стопу',closed_tp:'Закрыт по тейку',closed_time:'Закрыт по времени',entry_available:'Вход доступен'};
const entryStatusName:Record<string,string>={available:'Вход доступен',waiting:'Ожидает пересечения',late:'Вход упущен',late_entry:'Вход упущен',insufficient_rr:'Нет подходящей цены для входа',unavailable:'Нет актуальных данных',costs_unavailable:'Нет свежих данных о расходах',scenario_invalidated:'Сценарий отменён',target_reached:'Цель достигнута',waiting_confirmation:'Ждём подтверждения'};
const scalpStatusName:Record<string,string>={available:'План готов',waiting_data:'Ожидание свечей',waiting_breakout:'Ожидание подтверждения 5м',quote_stale:'Нет свежей котировки',book_stale:'Нет свежего стакана',book_not_subscribed:'Выберите наблюдение для загрузки стакана',book_loading:'Загрузка стакана',book_unavailable:'Нет соединения со стаканом',costs_unavailable:'Нет данных о расходах',no_stop_level:'Нет локального уровня для стопа',stop_too_tight:'Стоп слишком близко',stop_too_wide:'Стоп слишком далеко',no_target:'Нет близкой цели',insufficient_rr:'Мало места до цели',scenario_invalidated:'Цена прошла локальный стоп',target_reached:'Цена уже достигла цели',cancelled:'Наблюдение завершено'};
function entryAssessmentExplanation(assessment?:Signal['entry_assessment']){
  if(assessment?.status!=='insufficient_rr')return null;
  const ratio=assessment.required_net_rr==null?'заданный минимум':`${money(assessment.required_net_rr)}:1`;
  return `Даже при входе у границы пробоя возможная прибыль до цели слишком мала относительно риска до стопа. Требование — не менее ${ratio} после комиссии, проскальзывания и funding.`;
}
function earlyAssessmentReason(reason?:string){
  const detail=(reason||'').trim();
  return `\u041f\u0440\u0438\u0447\u0438\u043d\u0430: ${detail||'\u0446\u0435\u043d\u0430 \u0434\u043e\u0441\u0442\u0438\u0433\u043b\u0430 \u0443\u0440\u043e\u0432\u043d\u044f \u0441\u0442\u043e\u043f\u0430 \u0438\u043b\u0438 \u0432\u0435\u0440\u043d\u0443\u043b\u0430\u0441\u044c \u0437\u0430 \u0433\u0440\u0430\u043d\u0438\u0446\u0443 \u0441\u0446\u0435\u043d\u0430\u0440\u0438\u044f.'}`;
}
function earlyCancellationReason(s:Setup){
  const detail=(s.reason||'').trim();
  if(/\u0441\u0442\u043e\u043f/i.test(detail))return '\u041f\u0440\u0438\u0447\u0438\u043d\u0430: \u0446\u0435\u043d\u0430 \u0434\u043e\u0441\u0442\u0438\u0433\u043b\u0430 \u0443\u0440\u043e\u0432\u043d\u044f \u0441\u0442\u043e\u043f\u0430 \u0434\u043e \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f \u043f\u0440\u043e\u0431\u043e\u044f.';
  if(/\u0432\u0435\u0440\u043d\u0443\u043b|\u0432\u043d\u0443\u0442\u0440|\u0433\u0440\u0430\u043d\u0438\u0446/i.test(detail))return '\u041f\u0440\u0438\u0447\u0438\u043d\u0430: \u0446\u0435\u043d\u0430 \u0432\u0435\u0440\u043d\u0443\u043b\u0430\u0441\u044c \u0437\u0430 \u0433\u0440\u0430\u043d\u0438\u0446\u0443 \u0444\u0438\u0433\u0443\u0440\u044b, \u043f\u0440\u043e\u0431\u043e\u0439 \u043d\u0435 \u0443\u0434\u0435\u0440\u0436\u0430\u043b\u0441\u044f.';
  if(detail&&!/\u0441\u0446\u0435\u043d\u0430\u0440\u0438\u0439.*\u043e\u0442\u043c\u0435\u043d/i.test(detail))return `\u041f\u0440\u0438\u0447\u0438\u043d\u0430: ${detail}`;
  return '\u041f\u0440\u0438\u0447\u0438\u043d\u0430: \u0446\u0435\u043d\u0430 \u0434\u043e\u0448\u043b\u0430 \u0434\u043e \u0441\u0442\u043e\u043f\u0430 \u0434\u043e \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f \u043f\u0440\u043e\u0431\u043e\u044f.';
}
type PatternRiskGuidance={stop:string;target:string};
const patternRiskGuidance:Record<string,PatternRiskGuidance>={
  'ascending_triangle:long':{
    stop:'\u041f\u043e\u0434 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u043c \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0432\u043d\u0443\u0442\u0440\u0438 \u0444\u0438\u0433\u0443\u0440\u044b \u0438\u043b\u0438 \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0442\u0440\u0435\u0443\u0433\u043e\u043b\u044c\u043d\u0438\u043a\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  },
  'descending_triangle:short':{
    stop:'\u041d\u0430\u0434 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u043c \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0432\u043d\u0443\u0442\u0440\u0438 \u0444\u0438\u0433\u0443\u0440\u044b \u0438\u043b\u0438 \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0442\u0440\u0435\u0443\u0433\u043e\u043b\u044c\u043d\u0438\u043a\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0443\u044e \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443.'
  },
  'symmetric_triangle:long':{
    stop:'\u041f\u043e\u0434 \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0442\u0440\u0435\u0443\u0433\u043e\u043b\u044c\u043d\u0438\u043a\u0430 \u0438\u043b\u0438 \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0442\u0440\u0435\u0443\u0433\u043e\u043b\u044c\u043d\u0438\u043a\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  },
  'symmetric_triangle:short':{
    stop:'\u041d\u0430\u0434 \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0442\u0440\u0435\u0443\u0433\u043e\u043b\u044c\u043d\u0438\u043a\u0430 \u0438\u043b\u0438 \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0442\u0440\u0435\u0443\u0433\u043e\u043b\u044c\u043d\u0438\u043a\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0443\u044e \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443.'
  },
  'bull_flag:long':{
    stop:'\u041f\u043e\u0434 \u043d\u0438\u0436\u043d\u0435\u0439 \u0433\u0440\u0430\u043d\u0438\u0446\u0435\u0439 \u0444\u043b\u0430\u0433\u0430.',
    target:'\u041f\u0440\u043e\u0435\u043a\u0446\u0438\u044f \u0434\u0440\u0435\u0432\u043a\u0430 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f, \u0435\u0441\u043b\u0438 \u0446\u0435\u043b\u044c \u0434\u043e\u0441\u0442\u0438\u0436\u0438\u043c\u0430; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  },
  'bear_flag:short':{
    stop:'\u041d\u0430\u0434 \u0432\u0435\u0440\u0445\u043d\u0435\u0439 \u0433\u0440\u0430\u043d\u0438\u0446\u0435\u0439 \u0444\u043b\u0430\u0433\u0430.',
    target:'\u041f\u0440\u043e\u0435\u043a\u0446\u0438\u044f \u0434\u0440\u0435\u0432\u043a\u0430 \u0432\u043d\u0438\u0437 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f, \u0435\u0441\u043b\u0438 \u0446\u0435\u043b\u044c \u0434\u043e\u0441\u0442\u0438\u0436\u0438\u043c\u0430; \u0443\u0447\u0442\u0438 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0443\u044e \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443.'
  },
  'bull_pennant:long':{
    stop:'\u041f\u043e\u0434 \u043d\u0438\u0436\u043d\u0435\u0439 \u0433\u0440\u0430\u043d\u0438\u0446\u0435\u0439 \u0432\u044b\u043c\u043f\u0435\u043b\u0430.',
    target:'\u041f\u0440\u043e\u0435\u043a\u0446\u0438\u044f \u0434\u0440\u0435\u0432\u043a\u0430 \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  },
  'bear_pennant:short':{
    stop:'\u041d\u0430\u0434 \u0432\u0435\u0440\u0445\u043d\u0435\u0439 \u0433\u0440\u0430\u043d\u0438\u0446\u0435\u0439 \u0432\u044b\u043c\u043f\u0435\u043b\u0430.',
    target:'\u041f\u0440\u043e\u0435\u043a\u0446\u0438\u044f \u0434\u0440\u0435\u0432\u043a\u0430 \u0432\u043d\u0438\u0437 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0443\u044e \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443.'
  },
  'double_bottom:long':{
    stop:'\u041f\u043e\u0434 \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0432\u0442\u043e\u0440\u043e\u0433\u043e \u0434\u043d\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u043e\u0442 \u0434\u043d\u0430 \u0434\u043e \u043b\u0438\u043d\u0438\u0438 \u0448\u0435\u0438, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f \u043b\u0438\u043d\u0438\u0438 \u0448\u0435\u0438.'
  },
  'double_top:short':{
    stop:'\u041d\u0430\u0434 \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0432\u0442\u043e\u0440\u043e\u0439 \u0432\u0435\u0440\u0448\u0438\u043d\u044b.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u043e\u0442 \u0432\u0435\u0440\u0448\u0438\u043d\u044b \u0434\u043e \u043b\u0438\u043d\u0438\u0438 \u0448\u0435\u0438, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438.'
  },
  'head_shoulders:short':{
    stop:'\u041d\u0430\u0434 \u043f\u0440\u0430\u0432\u044b\u043c \u043f\u043b\u0435\u0447\u043e\u043c.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u043e\u0442 \u0433\u043e\u043b\u043e\u0432\u044b \u0434\u043e \u043b\u0438\u043d\u0438\u0438 \u0448\u0435\u0438, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f \u043b\u0438\u043d\u0438\u0438 \u0448\u0435\u0438; \u0443\u0447\u0442\u0438 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438.'
  },
  'inverse_head_shoulders:long':{
    stop:'\u041f\u043e\u0434 \u043f\u0440\u0430\u0432\u044b\u043c \u043f\u043b\u0435\u0447\u043e\u043c.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u043e\u0442 \u0433\u043e\u043b\u043e\u0432\u044b \u0434\u043e \u043b\u0438\u043d\u0438\u0438 \u0448\u0435\u0438, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f \u043b\u0438\u043d\u0438\u0438 \u0448\u0435\u0438; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  },
  'rectangle:long':{
    stop:'\u041f\u043e\u0434 \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u043a\u043e\u043d\u0441\u043e\u043b\u0438\u0434\u0430\u0446\u0438\u0438 \u0438\u043b\u0438 \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  },
  'rectangle:short':{
    stop:'\u041d\u0430\u0434 \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u043a\u043e\u043d\u0441\u043e\u043b\u0438\u0434\u0430\u0446\u0438\u0438 \u0438\u043b\u0438 \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0434\u0438\u0430\u043f\u0430\u0437\u043e\u043d\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u0442\u043e\u0447\u043a\u0438 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0443\u044e \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443.'
  },
  'rising_wedge:short':{
    stop:'\u041d\u0430\u0434 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u043c \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u043a\u043b\u0438\u043d\u0430 \u0438\u043b\u0438 \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0428\u0438\u0440\u0438\u043d\u0430 \u043a\u043b\u0438\u043d\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0438\u0435 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438.'
  },
  'falling_wedge:long':{
    stop:'\u041f\u043e\u0434 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u043c \u0437\u043d\u0430\u0447\u0438\u043c\u044b\u043c \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u043a\u043b\u0438\u043d\u0430 \u0438\u043b\u0438 \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0428\u0438\u0440\u0438\u043d\u0430 \u043a\u043b\u0438\u043d\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  },
  'ascending_channel:long':{
    stop:'\u041f\u043e\u0434 \u043d\u0438\u0436\u043d\u0435\u0439 \u0433\u0440\u0430\u043d\u0438\u0446\u0435\u0439 \u043a\u0430\u043d\u0430\u043b\u0430 \u0438\u043b\u0438 \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0428\u0438\u0440\u0438\u043d\u0430 \u043a\u0430\u043d\u0430\u043b\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f.'
  },
  'descending_channel:short':{
    stop:'\u041d\u0430\u0434 \u0432\u0435\u0440\u0445\u043d\u0435\u0439 \u0433\u0440\u0430\u043d\u0438\u0446\u0435\u0439 \u043a\u0430\u043d\u0430\u043b\u0430 \u0438\u043b\u0438 \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0428\u0438\u0440\u0438\u043d\u0430 \u043a\u0430\u043d\u0430\u043b\u0430, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u0431\u043b\u0438\u0436\u0430\u0439\u0448\u0443\u044e \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0443.',
  }
};
function getPatternRiskGuidance(pattern:string,side:string):PatternRiskGuidance{
  return patternRiskGuidance[`${pattern}:${side}`]??(side==='long'?{
    stop:'\u0417\u0430 \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u044b\u043c \u043c\u0438\u043d\u0438\u043c\u0443\u043c\u043e\u043c \u0444\u0438\u0433\u0443\u0440\u044b \u0438\u043b\u0438 \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0444\u0438\u0433\u0443\u0440\u044b, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u0432\u0435\u0440\u0445 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0441\u0432\u0435\u0440\u044c \u0441 \u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f\u043c\u0438.'
  }:{
    stop:'\u0417\u0430 \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u044b\u043c \u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\u043e\u043c \u0444\u0438\u0433\u0443\u0440\u044b \u0438\u043b\u0438 \u0440\u0435\u0442\u0435\u0441\u0442\u0430.',
    target:'\u0412\u044b\u0441\u043e\u0442\u0430 \u0444\u0438\u0433\u0443\u0440\u044b, \u043e\u0442\u043b\u043e\u0436\u0435\u043d\u043d\u0430\u044f \u0432\u043d\u0438\u0437 \u043e\u0442 \u043f\u0440\u043e\u0431\u043e\u044f; \u0443\u0447\u0442\u0438 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438.'
  });
}
function PatternRiskGuidance({pattern,side}:{pattern:string;side:string}){
  const guidance=getPatternRiskGuidance(pattern,side);
  return <div className="pattern-risk-guidance">
    <strong>{'\u041e\u0440\u0438\u0435\u043d\u0442\u0438\u0440 \u043f\u043e \u0444\u0438\u0433\u0443\u0440\u0435'}</strong>
    <span><b>SL:</b> {guidance.stop}</span>
    <span><b>TP:</b> {guidance.target}</span>
    <small>{'\u042d\u0442\u043e \u0433\u0435\u043e\u043c\u0435\u0442\u0440\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u043e\u0440\u0438\u0435\u043d\u0442\u0438\u0440: \u0443\u0447\u0438\u0442\u044b\u0432\u0430\u0439 \u0443\u0440\u043e\u0432\u043d\u0438 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438/\u0441\u043e\u043f\u0440\u043e\u0442\u0438\u0432\u043b\u0435\u043d\u0438\u044f; \u0446\u0438\u0444\u0440\u043e\u0432\u044b\u0435 SL \u0438 TP \u0432 \u043a\u0430\u0440\u0442\u043e\u0447\u043a\u0435 \u043e\u0441\u0442\u0430\u044e\u0442\u0441\u044f \u0440\u0430\u0441\u0447\u0451\u0442\u043d\u044b\u043c\u0438 \u0443\u0440\u043e\u0432\u043d\u044f\u043c\u0438.'}</small>
  </div>;
}
function setupEventName(setup:Setup){
  if(setup.status==='breakout_confirmed')return 'Пробой подтверждён';
  if(setup.preview_at&&Number(setup.event_at)===setup.preview_at)return 'Первое пересечение';
  if(setup.status==='cancelled')return 'Сценарий отменён';
  if(setup.status==='target_reached')return 'Цель достигнута';
  if(setup.status==='expired')return 'Наблюдение истекло';
  return 'Фигура сформирована';
}
let names:Record<string,string>={};
type IconName='brand'|'signals'|'early'|'pause'|'play'|'external'|'paper'|'up'|'down';
function Icon({name}:{name:IconName}){
  const paths:Record<IconName,React.ReactNode>={
    brand:<><path d="M4 19V9h4v10M10 19V5h4v14M16 19v-7h4v7"/><path d="M6 7V5m0 4v2m6-8V2m0 3v2m6 3V8m0 4v2"/></>,
    signals:<><path d="M3 12h4l3-7 4 14 3-7h4"/></>,
    early:<><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></>,
    pause:<><path d="M9 5v14M15 5v14"/></>,
    play:<><path d="m8 5 12 7-12 7z"/></>,
    external:<><path d="M14 4h6v6M20 4l-9 9"/><path d="M18 13v6H4V5h6"/></>,
    paper:<><path d="M12 3 4 6v5c0 5 3 8 8 10 5-2 8-5 8-10V6z"/><path d="m9 12 2 2 4-4"/></>,
    up:<><path d="m7 14 5-5 5 5"/></>,
    down:<><path d="m7 10 5 5 5-5"/></>,
  };
  return <svg className="icon" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">{paths[name]}</svg>;
}
async function api(path:string,method='GET',body?:unknown){
  const r=await fetch(`/api/${path}`,{method,headers:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)});
  if(!r.ok){const d=await r.json();throw Error(typeof d.detail==='string'?d.detail:'Проверьте значения настроек')}
  return r.json();
}
function mergeRows<T extends {id:string;event_at?:number;time?:number}>(current:T[],upsert:T[],remove:string[]=[]):T[]{
  const rows=new Map(current.map(row=>[row.id,row]));for(const id of remove)rows.delete(id);for(const row of upsert)rows.set(row.id,row);
  return [...rows.values()].sort((a,b)=>(b.event_at??b.time??0)-(a.event_at??a.time??0)||b.id.localeCompare(a.id));
}

function App(){
  const [snapshot,setSnapshot]=useState<Snapshot|null>(null);
  const [selected,setSelected]=useState<string|null>(null);
  const [selectedSetup,setSelectedSetup]=useState<string|null>(null);
  const [selectedSignalData,setSelectedSignalData]=useState<Signal|null>(null);
  const [selectedSetupData,setSelectedSetupData]=useState<Setup|null>(null);
  const [accountData,setAccountData]=useState<AccountFeed|null>(null);
  const [accountReview,setAccountReview]=useState<AccountReview|null>(null);
  const [accountSetups,setAccountSetups]=useState<any[]>([]);
  const [tab,setTab]=useState('signals');
  const [pair,setPair]=useState('BTCUSDT');
  const [tf,setTf]=useState('5');
  const [earlyPair,setEarlyPair]=useState('BTCUSDT');
  const [earlyTf,setEarlyTf]=useState('5');
  const selectedRef=useRef<string|null>(null); selectedRef.current=selected;
  const selectedSetupRef=useRef<string|null>(null);selectedSetupRef.current=selectedSetup;
  const tabRef=useRef(tab);tabRef.current=tab;
  const pairRef=useRef(pair);pairRef.current=pair;
  const tfRef=useRef(tf);tfRef.current=tf;
  const earlyPairRef=useRef(earlyPair);earlyPairRef.current=earlyPair;
  const earlyTfRef=useRef(earlyTf);earlyTfRef.current=earlyTf;
  const wsLastAt=useRef(0);const restInFlight=useRef(false);const restSequence=useRef(0);
  const [feedUpdatedAt,setFeedUpdatedAt]=useState(0);const [uiClock,setUiClock]=useState(Date.now());
  const panelAvailable=Boolean(snapshot&&feedUpdatedAt&&uiClock-feedUpdatedAt<6000);
  const [decisionContext,setDecisionContext]=useState<{id:string;value:DecisionEnvelope}|null>(null);
  const [setupContextData,setSetupContextData]=useState<{id:string;value:any}|null>(null);
  const lastSignalContextAt=useRef(0),lastSetupContextAt=useRef(0);
  const wsRef=useRef<WebSocket|null>(null);
  const lastClientMessageAt=useRef(0);const [renderToken,setRenderToken]=useState(0);
  const wsRevision=useRef<number|null>(null);const wsStream=useRef<string|null>(null);
  const [error,setError]=useState('');
  const [filter,setFilter]=useState('all');
  const [signalScope,setSignalScope]=useState<SignalScope>('current');
  const [olderSignals,setOlderSignals]=useState<Signal[]>([]);const [signalLimit,setSignalLimit]=useState(50);const [signalHasMore,setSignalHasMore]=useState(true);
  const [serverFilteredSignals,setServerFilteredSignals]=useState<Signal[]>([]);
  const [olderSetups,setOlderSetups]=useState<Setup[]>([]);const [setupLimit,setSetupLimit]=useState(50);const [setupHasMore,setSetupHasMore]=useState(true);
  useEffect(()=>{
    let active=true;
    setOlderSignals([]);setServerFilteredSignals([]);setSignalLimit(50);setSignalHasMore(true);
    if(filter==='all')return()=>{active=false};
    const query=new URLSearchParams({limit:'50',status:filter,view:'summary',signal_scope:signalScope});
    void api(`signals?${query}`).then((rows:Signal[])=>{
      if(active){setServerFilteredSignals(rows);setSignalHasMore(rows.length===50)}
    }).catch(e=>{if(active)setError(String(e))});
    return()=>{active=false};
  },[filter,signalScope]);
  useEffect(()=>{
    if(tab!=='signals'||signalLimit<=50)return;
    let active=true,inFlight=false;
    const refresh=async()=>{
      if(inFlight)return;
      inFlight=true;
      try{
        const limit=Math.min(signalLimit,1000);
        const query=new URLSearchParams({limit:String(limit),view:'summary',signal_scope:signalScope});
        if(filter!=='all')query.set('status',filter);
        const rows=await api(`signals?${query}`) as Signal[];
        if(!active)return;
        const oldest=rows.at(-1);
        const keepOlder=(items:Signal[])=>signalLimit<=1000?[]:items.filter(item=>{
          if(!oldest)return false;
          const t=item.event_at??item.time,cutoff=oldest.event_at??oldest.time;
          return t<cutoff||(t===cutoff&&item.id<oldest.id);
        });
        setOlderSignals(prev=>mergeRows(keepOlder(prev),rows));
        if(filter!=='all')setServerFilteredSignals(prev=>mergeRows(keepOlder(prev),rows));
        if(signalLimit<=1000)setSignalHasMore(rows.length===limit);
      }catch(e){if(active)setError(String(e))}finally{inFlight=false}
    };
    const timer=setInterval(()=>void refresh(),5000);
    return()=>{active=false;clearInterval(timer)};
  },[tab,signalLimit,signalScope,filter]);
  const [report,setReport]=useState<Report|null>(null);
  const [portfolio,setPortfolio]=useState<PortfolioReport|null>(null);
  const [research,setResearch]=useState<{progress:{stage:string;message:string}|null;report:ResearchReport|null;approval?:{status:string;eligible?:boolean;message?:string}}|null>(null);
  const [forecastOutcomes,setForecastOutcomes]=useState<ForecastOutcomesReport|null>(null);
  const [portfolioProgress,setPortfolioProgress]=useState<{stage:string;message:string}|null>(null);
  useEffect(()=>{
    let stopped=false;
    const refresh=()=>api('portfolio-backtest').then(r=>{if(!stopped){setPortfolio(r.report);setPortfolioProgress(r.progress)}}).catch(()=>{});
    void refresh();
    const timer=setInterval(refresh,10000);
    return ()=>{stopped=true;clearInterval(timer)};
  },[]);
  useEffect(()=>{let stopped=false;const refresh=()=>api('research').then(setResearch).catch(()=>{});void refresh();const timer=setInterval(()=>{if(!stopped)void refresh()},10000);return()=>{stopped=true;clearInterval(timer)}},[]);
  useEffect(()=>{let stopped=false;const refresh=()=>api('forecast-outcomes').then(setForecastOutcomes).catch(()=>{});void refresh();const timer=setInterval(()=>{if(!stopped)void refresh()},30000);return()=>{stopped=true;clearInterval(timer)}},[]);
  const [reportUrl,setReportUrl]=useState('');
  useEffect(()=>{
    if(!report){setReportUrl('');return}
    const url=URL.createObjectURL(new Blob([JSON.stringify(report,null,2)],{type:'application/json'}));
    setReportUrl(url);
    return ()=>URL.revokeObjectURL(url);
  },[report]);
  const [backtestStatus,setBacktestStatus]=useState('idle');
  const [days,setDays]=useState(7);
  useEffect(()=>{
    let stopped=false;let ws:WebSocket;let retry:ReturnType<typeof setTimeout>;
    wsLastAt.current=0;wsStream.current=null;wsRevision.current=null;restSequence.current++;
    setSnapshot(prev=>prev?({...prev,signals:[]}):null);
    setSelected(null);setSelectedSignalData(null);setDecisionContext(null);
    api('patterns').then(n=>{names=n}).catch(()=>{});
    async function refreshRest(){
      if(restInFlight.current||Date.now()-wsLastAt.current<5000)return;
      restInFlight.current=true;const seq=++restSequence.current;
      try{const value=await api(`snapshot?view=summary&signal_scope=${signalScope}`);
        if(!stopped&&seq===restSequence.current&&Date.now()-wsLastAt.current>=5000){
          setSnapshot(prev=>prev?({...value,
            signals:value.signals??[],
            setups:mergeRows(value.setups??[],prev.setups??[]).slice(0,50),
            trades:mergeRows(value.trades??[],prev.trades??[]).slice(-50)}):value);
          setFeedUpdatedAt(Date.now())
        }
      }catch(e){if(!stopped)setError(String(e))}finally{restInFlight.current=false}
    }
    void refreshRest();
    function connect(){
      ws=new WebSocket(`${location.protocol==='https:'?'wss':'ws'}://${location.host}/ws?v=3&signal_scope=${signalScope}`);
      wsRef.current=ws;
      ws.onopen=()=>{ws.send(JSON.stringify({type:'watch_signal',id:tabRef.current==='signals'?selectedRef.current:null}));ws.send(JSON.stringify({type:'watch_setup',id:tabRef.current==='early'?selectedSetupRef.current:null}));ws.send(JSON.stringify({type:'watch_account',enabled:tabRef.current==='account'}));const early=tabRef.current==='early';ws.send(JSON.stringify({type:'watch_chart',symbol:early?earlyPairRef.current:pairRef.current,timeframe:early?earlyTfRef.current:tfRef.current}))};
      ws.onmessage=e=>{if(stopped)return;const p=JSON.parse(e.data);lastClientMessageAt.current=performance.now();setRenderToken(lastClientMessageAt.current);if(p.type==='snapshot'){
          wsStream.current=p.stream_id??null;wsRevision.current=p.revision??null;wsLastAt.current=Date.now();setFeedUpdatedAt(Date.now());restSequence.current++;
          setSnapshot(prev=>prev?({...p,
            signals:p.signals??[],
            setups:mergeRows(p.setups??[],prev.setups??[]).slice(0,50),
            trades:mergeRows(p.trades??[],prev.trades??[]).slice(-50)}):p);
          if(p.account)setAccountData(p.account);
          if(p.context&&p.context_signal_id===selectedRef.current&&tabRef.current==='signals'){
            lastSignalContextAt.current=Date.now();setDecisionContext({id:p.context_signal_id,value:p.context});
          }
          if(p.setup_context&&p.setup_context_id===selectedSetupRef.current&&tabRef.current==='early'){
            lastSetupContextAt.current=Date.now();setSetupContextData({id:p.setup_context_id,value:p.setup_context});
            if(p.setup_context.setup)setSelectedSetupData(p.setup_context.setup);
          }
          setError('');return;
        }
        if(p.type==='delta'){
          if(wsStream.current!==p.stream_id||wsRevision.current==null||p.revision!==wsRevision.current+1){ws.send(JSON.stringify({type:'resync'}));return}
          wsRevision.current=p.revision;wsLastAt.current=Date.now();setFeedUpdatedAt(Date.now());restSequence.current++;
          if(p.signals_remove?.length){const removed=new Set<string>(p.signals_remove);setOlderSignals(prev=>prev.filter(row=>!removed.has(row.id)));setServerFilteredSignals(prev=>prev.filter(row=>!removed.has(row.id)))}
          setSnapshot(prev=>prev?({...prev,state:p.state,published_at:p.published_at,
            signals:mergeRows(prev.signals,p.signals_upsert,p.signals_remove),
            setups:mergeRows(prev.setups??[],p.setups_upsert,p.setups_remove),
            trades:mergeRows(prev.trades,p.trades_upsert,p.trades_remove),
            live_chart:p.live_chart??prev.live_chart}):null);
          if(p.account)setAccountData(p.account);
          if(p.context&&p.context_signal_id===selectedRef.current&&tabRef.current==='signals'){
            lastSignalContextAt.current=Date.now();setDecisionContext({id:p.context_signal_id,value:p.context});
          }
          if(p.setup_context&&p.setup_context_id===selectedSetupRef.current&&tabRef.current==='early'){
            lastSetupContextAt.current=Date.now();setSetupContextData({id:p.setup_context_id,value:p.setup_context});
            if(p.setup_context.setup)setSelectedSetupData(p.setup_context.setup);
          }
          setError('');
        }};
      ws.onclose=()=>{if(!stopped)retry=setTimeout(connect,2000)};
      ws.onerror=()=>ws.close();
    }connect();
    const poll=setInterval(()=>{if(Date.now()-wsLastAt.current>=5000)void refreshRest()},2000);
    const clock=setInterval(()=>setUiClock(Date.now()),1000);
    return ()=>{stopped=true;clearTimeout(retry);clearInterval(poll);clearInterval(clock);ws?.close()};
  },[signalScope]);
  useEffect(()=>{
    if(!renderToken)return;
    const frame=requestAnimationFrame(()=>{
      const render_ms=performance.now()-lastClientMessageAt.current;
      const socket=wsRef.current;
      if(socket?.readyState===WebSocket.OPEN)
        socket.send(JSON.stringify({type:'render_ack',render_ms}));
    });
    return()=>cancelAnimationFrame(frame);
  },[renderToken]);
  useEffect(()=>{if(wsRef.current?.readyState===WebSocket.OPEN){const early=tab==='early';wsRef.current.send(JSON.stringify({type:'watch_chart',symbol:early?earlyPair:pair,timeframe:early?earlyTf:tf}));wsRef.current.send(JSON.stringify({type:'watch_signal',id:tab==='signals'?selected:null}));wsRef.current.send(JSON.stringify({type:'watch_setup',id:tab==='early'?selectedSetup:null}));wsRef.current.send(JSON.stringify({type:'watch_account',enabled:tab==='account'}))}},[tab,pair,tf,earlyPair,earlyTf,selected,selectedSetup]);
  useEffect(()=>{
    if(tab!=='account')return;
    let stopped=false;let loading=false;
    const refresh=async()=>{
      if(loading)return;loading=true;
      try{const [trades,positions,status]=await Promise.all([api('account/trades'),api('account/positions'),api('account/status')]);
        if(!stopped)setAccountData({status:status.status,positions,orders:trades.orders??[],executions:trades.executions??[],updated_at:trades.updated_at??status.updated_at});
      }catch(e){if(!stopped)setError(String(e))}finally{loading=false}
    };
    void refresh();const timer=setInterval(()=>{if(Date.now()-wsLastAt.current>=5000)void refresh()},2000);
    return()=>{stopped=true;clearInterval(timer)};
  },[tab]);
  useEffect(()=>{
    if(!selected||tab!=='signals')return;
    let active=true;lastSignalContextAt.current=0;
    const refresh=()=>{
      if(Date.now()-lastSignalContextAt.current<5000)return;
      const id=selected;
      void api(`signals/${id}/context`).then(value=>{
        if(active&&selectedRef.current===id&&Date.now()-lastSignalContextAt.current>=5000){
          lastSignalContextAt.current=Date.now();setDecisionContext({id,value});
        }
      }).catch(()=>{});
    };
    refresh();const timer=setInterval(refresh,2000);
    return()=>{active=false;clearInterval(timer)};
  },[selected,tab]);
  useEffect(()=>{
    if(!selectedSetup||tab!=='early')return;
    let active=true;lastSetupContextAt.current=0;
    const refresh=()=>{
      if(Date.now()-lastSetupContextAt.current<5000)return;
      const id=selectedSetup;
      void api(`setups/${id}/context`).then(value=>{
        if(active&&selectedSetupRef.current===id&&Date.now()-lastSetupContextAt.current>=5000){
          lastSetupContextAt.current=Date.now();setSetupContextData({id,value});
          if(value.setup)setSelectedSetupData(value.setup);
        }
      }).catch(()=>{});
    };
    refresh();const timer=setInterval(refresh,2000);return()=>{active=false;clearInterval(timer)};
  },[selectedSetup,tab]);
  useEffect(()=>{
    if(backtestStatus!=='running')return;
    const timer=setInterval(()=>api('backtest').then(r=>{setBacktestStatus(r.status);setReport(r.result);if(r.error)setError(r.error)}).catch(e=>setError(e.message)),2000);
    return ()=>clearInterval(timer);
  },[backtestStatus]);
  async function action(fn:()=>Promise<unknown>){try{setError('');await fn()}catch(e){setError((e as Error).message)}}
  function openBybit(symbol:string){void action(()=>api(`open-bybit/${encodeURIComponent(symbol)}`,'POST'))}
  async function openAccountReview(execution:AccountExecution){
    setAccountReview(null);setAccountSetups([]);
    try{const [review,setups]=await Promise.all([api('account/trades/'+encodeURIComponent(execution.execId)),api('account/setups/'+encodeURIComponent(execution.symbol))]);
      setAccountReview(review);setAccountSetups(setups);
    }catch(e){setError(String(e))}
  }
  async function linkAccountTrade(execId:string,setupId:string){
    const result=await api('account/trades/'+encodeURIComponent(execId)+'/signal','PUT',{setup_id:setupId});
    setAccountReview(result);
    setAccountData(prev=>prev?({...prev,executions:prev.executions.map(x=>x.execId===execId?({...x,setup_id:setupId}):x)}):prev);
  }
  const state=snapshot?.state;
  const summarySig=selected?(snapshot?.signals.find(s=>s.id===selected)
    ??olderSignals.find(s=>s.id===selected)
    ??serverFilteredSignals.find(s=>s.id===selected)
    ??(selectedSignalData?.id===selected?selectedSignalData:null)):null;
  const sig=selected&&decisionContext?.id===selected?decisionContext.value.signal:summarySig;
  const allSetups=mergeRows(olderSetups,snapshot?.setups??[]);
  const chosenSetup=allSetups.find(s=>s.id===selectedSetup)
    ??(selectedSetupData?.id===selectedSetup?selectedSetupData:null);
  const activeSetups=allSetups.filter(s=>['wait_breakout','near_boundary','crossing'].includes(s.status));
  useEffect(()=>{
    if(!selectedSetup)return;
    const terminalStatuses=['breakout_confirmed','cancelled','expired','target_reached'];
    const selectedRows=[allSetups.find(s=>s.id===selectedSetup),
      selectedSetupData?.id===selectedSetup?selectedSetupData:null];
    if(selectedRows.some(s=>s&&terminalStatuses.includes(s.status))){
      setSelectedSetup(null);setSelectedSetupData(null);setSetupContextData(null);
    }
  },[selectedSetup,selectedSetupData?.id,selectedSetupData?.status,allSetups]);
  const setupGroups:Setup[][]=[];
  const pairedPatterns=['symmetric_triangle','rectangle','ascending_channel','descending_channel'];
  for(const item of activeSetups){
    const key=item.snapshot.shape_key??JSON.stringify(item.snapshot.pivots);
    const group=pairedPatterns.includes(item.pattern)?setupGroups.find(g=>g[0].pattern===item.pattern&&g[0].symbol===item.symbol&&g[0].timeframe===item.timeframe&&(g[0].snapshot.shape_key??JSON.stringify(g[0].snapshot.pivots))===key):undefined;
    if(group)group.push(item);else setupGroups.push([item]);
  }
  const setupEventKey=(group:Setup[])=>group.map(s=>[Number(s.event_at||s.detected_at||0),s.id] as const)
    .sort((a,b)=>b[0]-a[0]||b[1].localeCompare(a[1]))[0];
  setupGroups.sort((a,b)=>{const aa=setupEventKey(a),bb=setupEventKey(b);return bb[0]-aa[0]||bb[1].localeCompare(aa[1])});
  const activeSetupIds=new Set(activeSetups.map(s=>s.id));
  const setupMate=chosenSetup&&pairedPatterns.includes(chosenSetup.pattern)?allSetups.find(s=>s.pattern===chosenSetup.pattern&&s.symbol===chosenSetup.symbol&&s.timeframe===chosenSetup.timeframe&&s.side!==chosenSetup.side&&JSON.stringify(s.snapshot.pivots)===JSON.stringify(chosenSetup.snapshot.pivots)):null;
  function choose(s:Signal){setSelected(s.id);setSelectedSignalData(s);setPair(s.symbol);setTf(s.timeframe)}
  function chooseSetup(s:Setup){setSelectedSetup(s.id);setSelectedSetupData(s);setEarlyPair(s.symbol);setEarlyTf(s.timeframe)}
  const setupContext=setupContextData?.id===selectedSetup?setupContextData.value:null;
  const allSignals=mergeRows(mergeRows(serverFilteredSignals,olderSignals),snapshot?.signals??[]);
  useEffect(()=>{
    if(!selected)return;
    const latest=allSignals.find(signal=>signal.id===selected);
    if(latest&&latest!==selectedSignalData)setSelectedSignalData(latest);
  },[selected,snapshot?.signals,olderSignals,serverFilteredSignals,selectedSignalData]);
  const filteredSignals=allSignals.filter(s=>(signalScope==='all'||signalScopeAt(s,uiClock)===signalScope)&&
    (filter==='all'||(filter==='entry_available'?s.entry_assessment?.status==='available':s.status===filter)));
  const signals=filteredSignals.slice(0,signalLimit);
  async function loadMoreSignals(){
    const last=signals.at(-1);if(!last||!signalHasMore)return;
    const query=new URLSearchParams({limit:'50',before_time:String(last.event_at??last.time),before_id:last.id,view:'summary',signal_scope:signalScope});
    if(filter!=='all')query.set('status',filter);
    try{const rows=await api(`signals?${query}`) as Signal[];setOlderSignals(prev=>mergeRows(prev,rows));
      setSignalLimit(value=>value+50);setSignalHasMore(rows.length===50)}catch(e){setError(String(e))}
  }
  async function loadMoreSetups(){
    const visible=setupGroups.slice(0,setupLimit);const lastGroup=visible.at(-1);const last=lastGroup?.slice().sort((a,b)=>
      (a.event_at-b.event_at)||a.id.localeCompare(b.id))[0];if(!last||!setupHasMore)return;
    const query=new URLSearchParams({limit:'50',before_time:String(last.event_at),before_id:last.id,view:'summary'});
    query.set('status','wait_breakout,near_boundary,crossing');
    try{const rows=await api(`setups?${query}`) as Setup[];setOlderSetups(prev=>mergeRows(prev,rows));
      setSetupLimit(value=>value+50);setSetupHasMore(rows.length===50)}catch(e){setError(String(e))}
  }
  return <div className="shell">
    <header className="topbar"><div className="topbar-inner">
      <a className="brand" href="#top" aria-label="Контур"><Icon name="brand"/><span>контур</span><span className="brand-dot">.</span></a>
      <span className="paper-tag"><Icon name="paper"/> PAPER TRADING</span>
      <nav className="top-nav" aria-label={"\u0420\u0430\u0437\u0434\u0435\u043b\u044b \u0442\u0435\u0440\u043c\u0438\u043d\u0430\u043b\u0430"}>{[['signals','\u0421\u0438\u0433\u043d\u0430\u043b\u044b'],['early','\u0420\u0430\u043d\u043d\u0438\u0435 \u0441\u0438\u0433\u043d\u0430\u043b\u044b'],['account','\u041c\u043e\u0438 \u0441\u0434\u0435\u043b\u043a\u0438']].map(([id,label])=><button key={id} className={tab===id?'nav active':'nav'} aria-current={tab===id?'page':undefined} onClick={()=>setTab(id)}><Icon name={id==='signals'?'signals':id==='early'?'early':'paper'}/>{label}</button>)}</nav>
      <div className="topbar-status">
        <span className={`connection ${panelAvailable?'ok':''}`} title={`Панель: ${feedUpdatedAt?age(feedUpdatedAt):'ожидание'}`}><i/>{panelAvailable?'Панель онлайн':'Панель ожидает'}</span>
        <span className={`connection ${state?.connected?'ok':''}`} title={state?.error||state?.phase||''}><i/>{state?.connected?`Bybit · ${age(state.last_message)}`:`Bybit отключён · ${age(state?.last_message)}`}</span>
        <span className={`connection ${state?.spot_connection?.connected?'ok':''}`} title={state?.spot_connection?.error||''}><i/>{state?.spot_connection?.connected?`Спот · ${age(state.spot_connection.last_message)}`:`Спот отключён · ${age(state?.spot_connection?.last_message)}`}</span>
        <button className="button pause-button" aria-label={state?.paused?'Возобновить входы':'Пауза входов'} onClick={()=>action(()=>api(state?.paused?'resume':'pause','POST'))}><Icon name={state?.paused?'play':'pause'}/><span>{state?.paused?'Возобновить':'Пауза входов'}</span></button>
      </div>
    </div></header>
    <main id="top"><header className="workspace-heading"><div><div className="eyebrow">BYBIT / USDT PERPETUALS</div><h1>{tab==='signals'?'\u0424\u0438\u0433\u0443\u0440\u044b \u0441\u0442\u0430\u043d\u043e\u0432\u044f\u0442\u0441\u044f \u0441\u0438\u0433\u043d\u0430\u043b\u0430\u043c\u0438':tab==='early'?'\u0420\u0430\u043d\u043d\u0438\u0435 \u043d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u044f':tab==='account'?'\u041c\u043e\u0438 \u0441\u0434\u0435\u043b\u043a\u0438':tab==='trades'?'\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u0435 \u043a\u0430\u043f\u0438\u0442\u0430\u043b\u0430':tab==='backtest'?'\u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043d\u0430 \u0438\u0441\u0442\u043e\u0440\u0438\u0438':tab==='research'?'\u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043f\u0440\u0430\u0432\u0438\u043b \u043d\u0430 \u0438\u0441\u0442\u043e\u0440\u0438\u0438':'\u041f\u0440\u0430\u0432\u0438\u043b\u0430 \u0432\u0430\u0448\u0435\u0439 \u0441\u0442\u0440\u0430\u0442\u0435\u0433\u0438\u0438'}</h1></div></header>
      {state?.fee_sources?.some(source=>source.startsWith('bybit_vip0_standard'))&&<div className="notice">Для симуляции используется базовая комиссия Bybit VIP 0: 0,055% taker на вход и на выход. Регион и уровень аккаунта могут изменить фактическую ставку. Источник: <a href="https://www.bybit.com/en/help-center/article/Trading-Fee-Structure?category=top" target="_blank" rel="noreferrer">таблица комиссий Bybit</a>.</div>}
      {state?.fee_mode==='account'&&!state?.fee_sources?.includes('account')&&!state?.fee_error&&<div className="notice">Персональная комиссия Bybit ещё не загружена. Новые входы блокируются, пока не появится актуальная ставка аккаунта.</div>}
      {(error||state?.error||state?.fee_error||state?.open_interest_error||state?.day_halted)&&<div className="notice">{error||state?.error||state?.fee_error||state?.open_interest_error||'Достигнут дневной лимит убытка. Новые входы остановлены.'}{state?.fee_error&&<small>Введите read-only ключи в локальный .env и перезапустите сервис. Без актуальной комиссии входы блокируются.</small>}</div>}
      {tab==='signals'&&<ScannerHealth state={state} feedUpdatedAt={feedUpdatedAt} uiClock={uiClock}/>}
      {tab==='early'&&<EarlyPanel groups={setupGroups} chosenSetup={chosenSetup} setupMate={setupMate} context={setupContext} selectedId={selectedSetup} choose={chooseSetup} clearSelection={()=>{setSelectedSetup(null);setSelectedSetupData(null);setSetupContextData(null)}} pair={earlyPair} tf={earlyTf} setPair={setEarlyPair} setTf={setEarlyTf} symbols={state?.symbols??[]} liveChart={snapshot?.live_chart} feedUpdatedAt={feedUpdatedAt} visibleLimit={setupLimit} hasMore={setupHasMore} loadMore={loadMoreSetups}/>}
      {tab==='signals'&&<div className="signals-layout">
        <section className="panel signal-chart"><div className="panel-heading"><div><div className="eyebrow">СТРУКТУРА ЦЕНЫ</div><h2>{pair} <span className="subtle">/ {tfName(tf)}</span></h2></div><div className="controls">{sig&&<button className="button bybit-link" onClick={()=>openBybit(sig.symbol)}>Открыть фьючерс на Bybit <Icon name="external"/></button>}<select value={pair} onChange={e=>{setPair(e.target.value);setSelected(null)}}>{Array.from(new Set([pair,...(state?.symbols??[])])).map(s=><option key={s}>{s}</option>)}</select>{TIMEFRAMES.map(t=><button key={t} className={tf===t?'chip active':'chip'} onClick={()=>{setTf(t);setSelected(null)}}>{tfName(t)}</button>)}</div></div>
          <Chart key={`signal:${sig?.id??'latest'}:${sig?.lines?.length??0}:${sig?.pivots?.length??0}`} signal={sig} setup={null} symbol={pair} tf={tf} liveCandle={snapshot?.live_chart?.symbol===pair&&snapshot?.live_chart?.timeframe===tf?snapshot.live_chart.candle??null:null} closedCandle={snapshot?.live_chart?.symbol===pair&&snapshot?.live_chart?.timeframe===tf?snapshot.live_chart.closed_candle??null:null} feedUpdatedAt={feedUpdatedAt} context={decisionContext?.id===selected?decisionContext.value:null}/>
          {sig&&<div className="signal-detail"><strong>{names[sig.pattern]||sig.pattern}</strong><span>{sig.side==='long'?'LONG':'SHORT'}</span><PriceLevels entry={sig.entry??sig.boundary} stop={sig.stop} target={sig.target}/><PatternRiskGuidance pattern={sig.pattern} side={sig.side}/>{sig.filter_results?.market_context&&<small>{'\u0421\u0435\u0442\u0430\u043f'}: {sig.filter_results.market_context.scenario==='continuation'?'\u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0435\u043d\u0438\u0435':sig.filter_results.market_context.scenario==='reversal'?'\u0440\u0430\u0437\u0432\u043e\u0440\u043e\u0442':'\u043d\u0435\u0442 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f'} | {'1\u0447'} {sig.filter_results.market_context.hourly_bias??'\u2014'} | {'15\u043c'} {sig.filter_results.market_context.structure_15m?.direction??'\u2014'} | {'5\u043c'} {sig.filter_results.confirmation_5m?.passed?'\u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d':'\u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d'}</small>}{sig.filter_results?.detection_profile==='expanded'&&<small className="notice">Расширенный поиск: фигура показана для ручной оценки; геометрия может быть менее точной. Автоматический вход не разрешён.</small>}<span>RR {money(sig.rr)}</span><span>Риск {money(sig.risk)} USDT</span><span>Объем {price(sig.qty)}</span><span>Комиссия {money(sig.expected_fees)}</span><span>Funding резерв {money(sig.funding_reserve)}</span>{sig.entry_method&&<span>Вход: {sig.entry_method==='retest'?'ретест':'сразу'}</span>}{sig.filter_results?.trend&&<small>{'\u0422\u0440\u0435\u043d\u0434 1\u0447'}: EMA50 {price(sig.filter_results.trend.ema50)} / EMA200 {price(sig.filter_results.trend.ema200)} | {sig.filter_results.market_context?.hourly_bias==='up'?'\u0432\u0432\u0435\u0440\u0445':sig.filter_results.market_context?.hourly_bias==='down'?'\u0432\u043d\u0438\u0437':sig.filter_results.market_context?.hourly_bias??'\u043d/\u0434'}</small>}{sig.filter_results?.volume&&<small>{'\u041e\u0431\u043e\u0440\u043e\u0442 5m'}: {money(sig.filter_results.volume.multiple)}x {'\u043e\u0442 \u043c\u0435\u0434\u0438\u0430\u043d\u044b'} | {sig.filter_results.volume_pass?'\u043f\u0440\u043e\u0439\u0434\u0435\u043d':'\u043e\u0442\u043a\u0430\u0437'}</small>}{sig.filter_results?.volume&&<small>\u041e\u0431\u043e\u0440\u043e\u0442: {money(sig.filter_results.volume.breakout_turnover_usdt)} USDT / \u043c\u0438\u043d. {money(sig.filter_results.volume.minimum_notional_usdt)} USDT</small>}{sig.reason&&<small>{sig.reason}</small>}</div>}
        </section>
        <DecisionPanel data={decisionContext?.id===selected?decisionContext.value:null}/>
        <SignalFeed signals={signals} selected={selected} filter={filter} setFilter={setFilter} scope={signalScope} setScope={setSignalScope} choose={choose} openBybit={openBybit} loadMore={loadMoreSignals} hasMore={signalHasMore}/>
      </div>}
      {tab==='account'&&<AccountPanel data={accountData} review={accountReview} setups={accountSetups} selectExecution={openAccountReview} linkExecution={linkAccountTrade}/>}
      {tab==='trades'&&<><section className="panel"><div className="panel-heading"><h2>Открытые позиции</h2><span className="subtle">Нереализованный PnL: {money(state?.unrealized)} USDT</span></div><TradeTable trades={state?.positions??[]}/></section><section className="panel"><div className="panel-heading"><h2>Закрытые сделки</h2><span className="subtle">Funding всего: {money(state?.funding)} USDT</span></div><TradeTable trades={[...(snapshot?.trades??[])].reverse()}/></section></>}
      {tab==='settings'&&state&&<section className="panel settings"><h2>Лимиты симуляции</h2><p className="subtle">Баланс при создании: 1000 USDT · плечо 1? · топ-40 пар по суточному обороту. Изменения применяются к следующим входам.</p><form key={JSON.stringify(state.settings)} onSubmit={e=>{e.preventDefault();const f=new FormData(e.currentTarget);void action(()=>api('settings','PUT',{risk_fraction:Number(f.get('risk'))/100,max_positions:Number(f.get('positions')),daily_loss_fraction:Number(f.get('daily'))/100,slippage:Number(f.get('slip'))/100,min_rr:Number(f.get('rr'))}))}}>
        <label>Риск на сделку, %<input name="risk" type="number" step="0.1" min="0.1" max="2" defaultValue={state.settings.risk_fraction*100} required/></label><label>Максимум позиций<input name="positions" type="number" min="1" max="10" defaultValue={state.settings.max_positions} required/></label><label>Дневной лимит убытка, %<input name="daily" type="number" step="0.1" min="0.1" max="10" defaultValue={state.settings.daily_loss_fraction*100} required/></label><label>Проскальзывание на сторону, %<input name="slip" type="number" step="0.01" min="0" max="1" defaultValue={state.settings.slippage*100} required/></label><label>Минимальное чистое RR<input name="rr" type="number" step="0.1" min="1" max="10" defaultValue={state.settings.min_rr} required/></label><button className="button primary" type="submit">Сохранить правила</button></form><p className="subtle">Ставки комиссии загружены для {state.fees_ready} пар. Ключи API задаются только в локальном .env.</p></section>}
      {tab==='backtest'&&<section className="panel settings"><h2>Исторический прогон</h2><p className="subtle">Тот же движок стратегии, минутные свечи для исполнения и фактический funding. Текущая ставка аккаунта применяется ко всему периоду. Загрузка может занять несколько минут.</p><div className="controls"><input aria-label="Пара" value={pair} onChange={e=>setPair(e.target.value.toUpperCase())}/><select value={tf} onChange={e=>setTf(e.target.value)}>{TIMEFRAMES.map(t=><option key={t} value={t}>{tfName(t)}</option>)}</select><label>Дней <input type="number" min="1" max="30" value={days} onChange={e=>setDays(Number(e.target.value))}/></label><button className="button primary" disabled={backtestStatus==='running'} onClick={()=>action(async()=>{await api('backtest','POST',{symbol:pair,timeframe:tf,days});setReport(null);setBacktestStatus('running')})}>{backtestStatus==='running'?'Выполняется…':'Запустить прогон'}</button></div>{report&&<><div className="metrics report">{[['Сделок',report.trades_count],['Прибыльных',`${money(report.win_rate*100)} %`],['Результат',`${money(report.net)} USDT`],['Макс. просадка',`${money(report.max_drawdown*100)} %`]].map(([l,v])=><div className="metric" key={l}><span>{l}</span><strong>{v}</strong></div>)}</div><p className="subtle">{report.execution_model}. Незакрытых позиций: {report.open_positions.length}.</p><a className="button" href={reportUrl} download="backtest.json">Скачать отчет JSON</a><TradeTable trades={report.trades}/></>}</section>}
      {tab==='backtest'&&<PortfolioPanel report={portfolio} progress={portfolioProgress}/>}
      {tab==='research'&&<ResearchPanel data={research} forecastOutcomes={forecastOutcomes}/>}
      <footer><span><Icon name="paper"/> Симуляция · реальные ордера не отправляются</span><span>Комиссии и funding учитываются. Прибыль не гарантирована.</span></footer>
    </main></div>;
}

function SignalTiming({signal}:{signal:Signal}){
  const delivery=signal.filter_results?.delivery;
  const received=delivery?.timing_version===2?(delivery.received_at??signal.received_at):undefined;
  const detected=delivery?.detected_at??signal.detected_at;
  const published=delivery?.timing_version===2?(delivery.published_at??signal.published_at):undefined;
  const publishDelay=delivery?.timing_version===2?(delivery.publish_delay_ms??(published?Math.max(0,published-(signal.event_at??signal.time)):undefined)):undefined;
  return <small className="signal-delay">Событие {date(signal.event_at??signal.time)} · получено {received?date(received):'нет данных'} · обнаружено {detected?date(detected):'нет данных'}{published?` · опубликовано ${date(published)}`:''}{publishDelay!=null?` · задержка публикации ${Math.round(publishDelay/1000)}с`:delivery?.delay_ms!=null?` · задержка обнаружения ${Math.round(delivery.delay_ms/1000)}с`:''}{delivery?.recovered?' · восстановлено из истории':''}</small>;
}

function SignalFeed({signals,selected,filter,setFilter,scope,setScope,choose,openBybit,loadMore,hasMore}:{signals:Signal[];selected:string|null;filter:string;setFilter:(value:string)=>void;scope:SignalScope;setScope:(value:SignalScope)=>void;choose:(signal:Signal)=>void;openBybit:(symbol:string)=>void;loadMore:()=>void;hasMore:boolean}){
  return <section className="panel signal-feed"><div className="panel-heading"><h2>Лента сигналов <span className="count">{signals.length}</span></h2><select aria-label="Выбор ленты сигналов" value={scope} onChange={e=>setScope(e.target.value as SignalScope)}><option value="current">Актуальные</option><option value="history">История</option><option value="all">Все сигналы</option></select><select aria-label="Статус сигнала" value={filter} onChange={e=>setFilter(e.target.value)}><option value="all">Все статусы</option>{Object.entries(statusName).map(([value,label])=><option key={value} value={value}>{label}</option>)}</select></div>
    <div className="signal-card-list">{signals.map(signal=><article key={signal.id} className={`signal-card${selected===signal.id?' selected':''}`} role="button" tabIndex={0} onClick={()=>choose(signal)} onKeyDown={event=>{if(event.target===event.currentTarget&&(event.key==='Enter'||event.key===' ')){event.preventDefault();choose(signal)}}}>
      <div className="signal-card-top"><b>{signal.symbol}</b><span className={`direction ${signal.side}`}><Icon name={signal.side==='long'?'up':'down'}/>{signal.side==='long'?'LONG':'SHORT'}</span><small>{date(signal.event_at??signal.time)}</small></div>
      <strong className="signal-card-pattern">{names[signal.pattern]||signal.pattern}</strong>
      <SignalTiming signal={signal}/>
      <small className="signal-delay">Котировка: {signal.entry_assessment?.quote_age_ms==null?'нет данных':`${(signal.entry_assessment.quote_age_ms/1000).toFixed(1)}с назад`}</small>
      <div className="signal-card-meta"><span>{tfName(signal.timeframe)}</span><span>Пробой {price(signal.boundary)}</span><span>Сейчас {price(signal.entry_assessment?.price)}</span><span>Вход {signal.entry_assessment?.price_min==null?'—':`${price(signal.entry_assessment.price_min)} – ${price(signal.entry_assessment.price_max)}`}</span><span>SL {price(signal.stop)} · TP {price(signal.target)}</span></div>
      <PatternRiskGuidance pattern={signal.pattern} side={signal.side}/>
      <div className="signal-card-bottom"><span className={`status ${signal.entry_assessment?.status??signal.status}`}>{entryStatusName[signal.entry_assessment?.status??'']??statusName[signal.status]}</span><span className={signal.expected_net==null?'subtle':signal.expected_net>=0?'positive':'negative'}>{signal.expected_net==null?'Чистый результат н/д':`${money(signal.expected_net)} USDT`}</span><button type="button" className="signal-exchange-link" title={`Открыть ${signal.symbol} на Bybit в Chrome`} onClick={event=>{event.stopPropagation();openBybit(signal.symbol)}}>Bybit <Icon name="external"/></button></div>
      {entryAssessmentExplanation(signal.entry_assessment)&&<small className="signal-card-reason">{entryAssessmentExplanation(signal.entry_assessment)}</small>}
      {signal.market_background_summary&&<small className="signal-card-reason market-background-summary">{signal.market_background_summary}</small>}
      {signal.reason&&<small className="signal-card-reason">{signal.reason}</small>}
    </article>)}{!signals.length&&<div className="empty"><span className="empty-icon"><Icon name="signals"/></span><h3>{scope==='history'?'В истории пока нет сигналов':'Ждём подтверждённую фигуру'}</h3><p>{scope==='history'?'Здесь появятся восстановленные и устаревшие сигналы.':'Сканируем выбранные пары на 5м, 15м, 1ч и 4ч.'}</p></div>}{hasMore&&<button className="button load-more" onClick={loadMore}>Загрузить ещё</button>}</div>
  </section>;
}

function ScannerHealth({state,feedUpdatedAt,uiClock}:{state:State|undefined;feedUpdatedAt:number;uiClock:number}){
  const d=state?.scanner_diagnostics;
  // The scanner can still be processing a recovery/subscription phase while
  // its market streams are already delivering fresh data. Judge freshness by
  // the actual ticker and kline receive times instead of the phase label.
  const scanning=Boolean(state?.connected&&d?.last_price_received_at&&d?.last_kline_received_at&&
    uiClock-d.last_price_received_at<=30_000&&uiClock-d.last_kline_received_at<=30_000);
  const recovery=d?.candle_recovery;
  const message=state?.phase==='waiting_network'?'Ожидание подключения к Bybit. История восстановится автоматически.':!scanning?`Нет свежих данных Bybit${state?.error?`: ${state.error}`:'.'} Показываются сохранённые сигналы.`:d?.live_warming_streams?`Подготовка данных для ${d.live_warming_streams} потоков.`:d?.stale_streams?.length?`Задержаны закрытые свечи: ${d.stale_streams.map(x=>`${x.symbol} ${tfName(x.timeframe)}`).join(', ')}. Запрошено восстановление истории.`:(d?.new_breakouts_last_hour??0)===0?`Сканирование работает — новых подтверждений пока нет (${d?.window_minutes??0} мин. наблюдения).`:`Сканирование работает — за последний час найдено новых пробоев: ${d?.new_breakouts_last_hour??0}.`;
  return <section className={`scanner-health ${scanning&&!d?.stale_streams?.length&&!d?.live_warming_streams?'healthy':'attention'}`}>
    <strong>{message}</strong><div className="scanner-times"><span>Список получен: {feedUpdatedAt?`${age(feedUpdatedAt)} · ${date(feedUpdatedAt)}`:'ожидание'} </span><span>Проверка свечей: {d?.last_closed_candle_scan?`${age(d.last_closed_candle_scan)} · ${date(d.last_closed_candle_scan)}`:'ожидание'}</span><span>Последний сигнал: {d?.last_signal_at?`${age(d.last_signal_at)} · ${date(d.last_signal_at)}`:'пока не найден'}</span></div>
    {d&&<small>Проверено закрытых свечей: {d.checked_closed_candles_last_hour} · найдено пробоев: {d.raw_breakouts_last_hour} · новые: {d.new_breakouts_last_hour} · потоки: {d.streams_current}/{d.streams_expected}{d.window_minutes<60?` · накоплено ${d.window_minutes} мин.`:''} · последнее изменение цены {d.last_price_received_at?`${age(d.last_price_received_at)} назад`:'нет данных'} · соединение {state?.connected?'работает':'восстанавливается'} · пакет панели {feedUpdatedAt?`${Math.max(0,Math.floor((uiClock-feedUpdatedAt)/1000))}с назад`:'ожидание'}</small>}{d&&(d.live_detection_queue_depth!=null||d.history_detection_queue_depth!=null||recovery?.status==='recovering'||recovery?.status==='waiting_network')&&<small>Очередь актуальных фигур: {d.live_detection_queue_depth??'нет данных'} · очередь истории: {d.history_detection_queue_depth??'нет данных'} · восстановление потоков: {d.history_pending_streams??recovery?.pending_streams??0}{recovery?.status==='waiting_network'?' · история ожидает подключения к Bybit':''}</small>}{d&&<small>Задержки p95, мс: Bybit → сервер {d.network_delay_p95_ms??0} · очередь свечей {d.candle_queue_wait_p95_ms??0} · подтверждение фигуры {d.live_detection_latency_p95_ms??0} · запись БД {d.database_write_p95_ms??0} · сервер → WebSocket {d.socket_delivery_p95_ms??0} · браузерный кадр {d.browser_render_p95_ms??0}</small>}
  </section>;
}

function scalpStatus(setup:Setup){
  const plan=setup.current?.scalp_plan;
  if(!plan)return '\u041e\u0436\u0438\u0434\u0430\u0435\u0442\u0441\u044f \u0440\u0430\u0441\u0447\u0451\u0442 \u043f\u043b\u0430\u043d\u0430';
  if(setup.status==='crossing'&&plan.preliminary)return '\u041f\u0440\u0435\u0434\u0432\u0430\u0440\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u0439 \u043f\u0440\u043e\u0431\u043e\u0439 \u00b7 '+(scalpStatusName[plan.status]??plan.status);
  return scalpStatusName[plan.status]??plan.status;
}

function SetupChoice({setup,selected,onClick}:{setup:Setup;selected:boolean;onClick:()=>void}){
  const plan=setup.current?.scalp_plan;
  const boundary=plan?.boundary??setup.current?.boundary??setup.snapshot.boundary;
  return <button type="button" className={'setup-choice '+(selected?'selected':'')} onClick={onClick}>
    <span className={'direction '+setup.side}>{setup.side==='long'?'\u041b\u041e\u041d\u0413':'\u0428\u041e\u0420\u0422'}</span>
    <strong>{scalpStatus(setup)}</strong>
    <small>{setup.side==='long'?'\u0413\u0440\u0430\u043d\u0438\u0446\u0430 \u0432\u044b\u0448\u0435':'\u0413\u0440\u0430\u043d\u0438\u0446\u0430 \u043d\u0438\u0436\u0435'} {price(boundary)} \u00b7 {plan?.preliminary?'\u043f\u0440\u0435\u0434\u0432\u0430\u0440\u0438\u0442\u0435\u043b\u044c\u043d\u043e':'\u043f\u043e \u043f\u043b\u0430\u043d\u0443'}</small>
    <small>{'\u0412\u0445\u043e\u0434'} {price(plan?.entry)} \u00b7 SL {price(plan?.stop)} \u00b7 TP {price(plan?.target)} \u00b7 RR {plan?.net_rr==null?'\u2014':money(plan.net_rr)+':1'}</small>
    {plan?.reason&&<small>{plan.reason}</small>}
    {setup.current?.market_background_summary&&<small className="market-background-summary">{setup.current.market_background_summary}</small>}
  </button>;
}

function EarlyPanel({groups,chosenSetup,setupMate,context,selectedId,choose,clearSelection,pair,tf,setPair,setTf,symbols,liveChart,feedUpdatedAt,visibleLimit,hasMore,loadMore}:{groups:Setup[][];chosenSetup:Setup|null;setupMate?:Setup|null;context:any;selectedId:string|null;choose:(s:Setup)=>void;clearSelection:()=>void;pair:string;tf:string;setPair:(s:string)=>void;setTf:(s:string)=>void;symbols:string[];liveChart:Snapshot['live_chart'];feedUpdatedAt:number;visibleLimit:number;hasMore:boolean;loadMore:()=>void}){
  const plan=context?.current?.scalp_plan as ScalpPlan|undefined;
  const selectedCurrent=context?.setup??chosenSetup;
  const selectedChoice=selectedCurrent?({...selectedCurrent,current:{...(selectedCurrent.current??{}),scalp_plan:plan,boundary:context?.current?.boundary}} as Setup):null;
  const sideLabel=(side:string)=>side==='long'?'\u043b\u043e\u043d\u0433':'\u0448\u043e\u0440\u0442';
  const trendLabel=(value:string|null|undefined)=>value==='up'?'\u0432\u0432\u0435\u0440\u0445':value==='down'?'\u0432\u043d\u0438\u0437':value==='mixed'?'\u0441\u043c\u0435\u0448\u0430\u043d\u043d\u043e':'\u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d';
  return <div className="early-layout">
    <section className="panel early-feed">
      <div className="panel-heading"><div><div className="eyebrow">{'\u041d\u0410\u0411\u041b\u042e\u0414\u0415\u041d\u0418\u0415 \u0411\u0415\u0417 \u0410\u0412\u0422\u041e\u0412\u0425\u041e\u0414\u0410'}</div><h2>{'\u0414\u043e \u043f\u0440\u043e\u0431\u043e\u044f'} <span className="count">{groups.length}</span></h2></div><span className="subtle">{'\u041d\u043e\u0432\u044b\u0435 \u043d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u044f \u0441\u043d\u0430\u0447\u0430\u043b\u0430'}</span></div>
      <div className="setup-list">{groups.slice(0,visibleLimit).map(group=>{
        const eventSetup=group.slice().sort((a,b)=>(Number(b.event_at||b.detected_at||0)-Number(a.event_at||a.detected_at||0))||b.id.localeCompare(a.id))[0];
        const eventAt=eventSetup.event_at||eventSetup.detected_at;
        const formedAt=eventSetup.delivery?.exchange_event_at??eventSetup.detected_at;
        const eventTime=setupEventName(eventSetup)+' '+date(eventAt)+(formedAt!==eventAt?' \u00b7 \u0444\u0438\u0433\u0443\u0440\u0430 \u0441\u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u043d\u0430 '+date(formedAt):'');
        return <article key={eventSetup.id} className="setup-card">
          <header><span><b>{eventSetup.symbol}</b><small>{eventTime} \u00b7 {tfName(eventSetup.timeframe)}{eventSetup.recovered?' \u00b7 \u0432\u043e\u0441\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\u043e \u043f\u043e \u0438\u0441\u0442\u043e\u0440\u0438\u0438':''}</small></span><b>{names[eventSetup.pattern]||eventSetup.pattern}</b></header>
          <div className="setup-side-options">{group.slice().sort((a,b)=>a.side==='long'?-1:b.side==='long'?1:a.id.localeCompare(b.id)).map(s=><SetupChoice key={s.id} setup={s} selected={selectedId===s.id} onClick={()=>choose(s)}/>)}</div>
        </article>;
      })}{!groups.length&&<div className="empty"><h3>{'\u041f\u043e\u043a\u0430 \u043d\u0435\u0442 \u0441\u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0445 \u0444\u0438\u0433\u0443\u0440 \u043f\u0435\u0440\u0435\u0434 \u043f\u0440\u043e\u0431\u043e\u044f\u043c\u0438'}</h3><p>{'\u0418\u0449\u0435\u043c \u0433\u0435\u043e\u043c\u0435\u0442\u0440\u0438\u044e \u043f\u043e \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d\u043d\u044b\u043c \u044d\u043a\u0441\u0442\u0440\u0435\u043c\u0443\u043c\u0430\u043c. \u0412\u0435\u0440\u0448\u0438\u043d\u044b \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0430\u044e\u0442\u0441\u044f \u0442\u0440\u0435\u043c\u044f \u0441\u0432\u0435\u0447\u0430\u043c\u0438 \u0441\u043f\u0440\u0430\u0432\u0430.'}</p></div>}</div>{hasMore&&<button className="button load-more" onClick={loadMore}>{'\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u044c \u0435\u0449\u0451'}</button>}
    </section>
    <section className="panel early-chart">
      <div className="panel-heading"><div><div className="eyebrow">{'\u0413\u0420\u0410\u0424\u0418\u041a \u041d\u0410\u0411\u041b\u042e\u0414\u0415\u041d\u0418\u042f'}</div><h2>{chosenSetup?chosenSetup.symbol+' \u00b7 '+(names[chosenSetup.pattern]||chosenSetup.pattern):pair+' / '+tfName(tf)}</h2></div><div className="controls"><select aria-label="????????" value={pair} onChange={e=>{setPair(e.target.value);clearSelection()}}>{Array.from(new Set([pair,...symbols])).map(s=><option key={s}>{s}</option>)}</select>{TIMEFRAMES.map(t=><button key={t} className={tf===t?'chip active':'chip'} onClick={()=>{setTf(t);clearSelection()}}>{tfName(t)}</button>)}</div></div>
      {chosenSetup?<><Chart key={`setup:${selectedCurrent?.id??'none'}:${selectedCurrent?.snapshot.lines.length??0}:${selectedCurrent?.snapshot.pivots.length??0}`} signal={null} setup={selectedCurrent} symbol={pair} tf={tf} liveCandle={liveChart?.symbol===pair&&liveChart?.timeframe===tf?liveChart.candle:null} closedCandle={liveChart?.symbol===pair&&liveChart?.timeframe===tf?liveChart.closed_candle:null} feedUpdatedAt={feedUpdatedAt} context={context}/>{!context&&<p className="chart-loading">{'\u0417\u0430\u0433\u0440\u0443\u0436\u0430\u0435\u043c \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442 \u0432\u044b\u0431\u0440\u0430\u043d\u043d\u043e\u0433\u043e \u043d\u0430\u0431\u043b\u044e\u0434\u0435\u043d\u0438\u044f\u2026'}</p>}{context&&selectedCurrent&&<div className="signal-detail">
        <strong>{names[selectedCurrent.pattern]||selectedCurrent.pattern} \u00b7 {sideLabel(selectedCurrent.side)}</strong>
        {setupMate&&<div className="setup-switch"><span>{'\u041f\u0430\u0440\u043d\u044b\u0439 \u0441\u0446\u0435\u043d\u0430\u0440\u0438\u0439:'}</span>{selectedChoice&&<SetupChoice setup={selectedChoice} selected onClick={()=>choose(selectedCurrent!)}/>}<SetupChoice setup={setupMate} selected={false} onClick={()=>choose(setupMate)}/></div>}
        <small>{'\u0424\u0438\u0433\u0443\u0440\u0430 \u0437\u0430\u0434\u0430\u0451\u0442 \u043d\u0430\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435 \u0438 \u0433\u0440\u0430\u043d\u0438\u0446\u0443. \u0421\u043a\u0430\u043b\u044c\u043f-\u043f\u043b\u0430\u043d \u0431\u0435\u0440\u0451\u0442 \u0432\u0445\u043e\u0434 \u043f\u043e ask \u0434\u043b\u044f \u043b\u043e\u043d\u0433\u0430 \u0438 bid \u0434\u043b\u044f \u0448\u043e\u0440\u0442\u0430; \u0433\u043e\u0440\u0438\u0437\u043e\u043d\u0442 \u0441\u0434\u0435\u043b\u043a\u0438 \u2014 5\u201330 \u043c\u0438\u043d\u0443\u0442.'}</small>
        <ScalpPlanCard plan={plan}/>
        <details className="figure-plan-details"><summary>{'\u0423\u0440\u043e\u0432\u043d\u0438 \u0444\u0438\u0433\u0443\u0440\u044b \u0438 \u0434\u0440\u0443\u0433\u0438\u0435 \u0441\u0446\u0435\u043d\u0430\u0440\u0438\u0438'}</summary>
          <PriceLevels entry={context.current?.entry_assessment?.price??context.current?.boundary??selectedCurrent.snapshot.boundary} stop={selectedCurrent.snapshot.stop} target={selectedCurrent.snapshot.target}/>
          <span>{'\u0413\u0440\u0430\u043d\u0438\u0446\u0430 \u0444\u0438\u0433\u0443\u0440\u044b:'} {price(context.current?.boundary??selectedCurrent.snapshot.boundary)} \u00b7 ATR {'\u0444\u0438\u0433\u0443\u0440\u044b'} {price(selectedCurrent.snapshot.atr)}</span>
          <EntryPlans plans={context.current?.entry_plans}/>
        </details>
      </div>}</>:<div className="empty"><h3>{'\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0441\u043e\u0431\u044b\u0442\u0438\u0435 \u0432 \u043b\u0435\u043d\u0442\u0435'}</h3><p>{'\u0421\u0432\u0435\u0447\u0438, \u0433\u0440\u0430\u043d\u0438\u0446\u044b \u0444\u0438\u0433\u0443\u0440\u044b, \u0441\u0442\u043e\u043f \u0438 \u0446\u0435\u043b\u044c \u043f\u043e\u044f\u0432\u044f\u0442\u0441\u044f \u0437\u0434\u0435\u0441\u044c.'}</p></div>}
    </section>
  </div>;
}

function EntryPlans({plans}:{plans?:EntryPlan[]}){
  if(!plans?.length)return <p className="subtle">План входа пока недоступен.</p>;
  if(!Array.isArray(plans)||!plans.length)return null;
  const titles:Record<string,string>={early_breakout:'Предварительное пересечение',confirmed_breakout:'Подтверждённый пробой',retest:'Ретест'};
  const statuses:Record<string,string>={preliminary:'Текущая свеча пересекла границу',waiting:'Ожидание пробоя',waiting_confirmation:'Ожидание ретеста',confirmed:'Подтверждён',available:'Условия выполнены',already_confirmed:'Свеча уже закрылась',unavailable:'Расчёт недоступен',late:'Цена вышла за зону входа',expired:'Истёк',cancelled:'Отменён',insufficient_data:'Недостаточно данных',insufficient_rr:'Нет подходящей цены для входа: потенциал прибыли ниже минимума',late_entry:'Потенциал прибыли ниже минимума',scenario_invalidated:'Сценарий отменён',target_reached:'Цель достигнута',costs_unavailable:'Нет данных о расходах'};
  return <div className="entry-plans">{plans.map(p=><article className="entry-plan" key={p.scenario}><b>{titles[p.scenario]??p.scenario}</b><strong>{statuses[p.status]??p.status}</strong>{p.execution&&<small>{p.execution.ready?'Исполнение доступно':'Исполнение заблокировано'} | spread {p.execution.orderbook?.spread_bps==null?'—':money(p.execution.orderbook.spread_bps)} bps | fill {p.execution.orderbook?.estimated_fill_ratio==null?'—':money(p.execution.orderbook.estimated_fill_ratio*100)+'%'}</small>}{p.activation&&<span>Условие: {p.activation}</span>}<span>Вход: {price(p.price_min)} – {price(p.price_max)}</span><span>Стоп {price(p.stop)} · первая цель {price(p.target)} · проекция {price(p.projection)}</span><span>Чистое RR: {p.net_rr==null?'н/д':`${money(p.net_rr)}:1`}</span>{p.reason&&<small className={p.status==='cancelled'||p.status==='scenario_invalidated'?'plan-reason':undefined}>{p.status==='cancelled'||p.status==='scenario_invalidated'?earlyAssessmentReason(p.reason):p.reason}</small>}{p.cancel_if&&<small>Отмена: {p.cancel_if}</small>}</article>)}</div>;
}

function ScalpPlanCard({plan}:{plan?:ScalpPlan}){
  if(!plan)return <article className="entry-plan scalp-plan"><b>{'\u0421\u043a\u0430\u043b\u044c\u043f-\u043f\u043b\u0430\u043d \u00b7 5\u201330 \u043c\u0438\u043d\u0443\u0442'}</b><strong>{'\u041e\u0436\u0438\u0434\u0430\u043d\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0445'}</strong><small>{'\u041f\u043b\u0430\u043d \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0438 \u0441\u0432\u0435\u0447\u0435\u0439, \u043a\u043e\u0442\u0438\u0440\u043e\u0432\u043a\u0438 \u0438 \u0441\u0442\u0430\u043a\u0430\u043d\u0430.'}</small></article>;
  const status=scalpStatusName[plan.status]??plan.status;
  const market=plan.market_context;
  const trendName=(value:string|null|undefined)=>value==='up'?'\u0432\u0432\u0435\u0440\u0445':value==='down'?'\u0432\u043d\u0438\u0437':value==='mixed'?'\u0441\u043c\u0435\u0448\u0430\u043d\u043d\u043e':'\u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d';
  return <article className={'entry-plan scalp-plan '+(plan.status==='available'?'ready':'')}>
    <b>{'\u0421\u043a\u0430\u043b\u044c\u043f-\u043f\u043b\u0430\u043d \u00b7 5\u201330 \u043c\u0438\u043d\u0443\u0442'} \u00b7 {plan.side==='long'?'\u043b\u043e\u043d\u0433':'\u0448\u043e\u0440\u0442'}</b>
    <strong>{status}</strong>
    {plan.preliminary&&<small className="plan-reason">{'\u041f\u0440\u0435\u0434\u0432\u0430\u0440\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u0439 \u043f\u043b\u0430\u043d \u2014 \u043f\u0440\u043e\u0431\u043e\u0439 \u043d\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d.'} {plan.confirmation_reason}</small>}
    <PriceLevels entry={plan.entry} stop={plan.stop} target={plan.target}
      entryMeta={'\u0433\u0440\u0430\u043d\u0438\u0446\u0430 '+price(plan.boundary)}
      stopMeta={plan.stop_pct==null?'':money(plan.stop_pct)+'% \u043e\u0442 \u0432\u0445\u043e\u0434\u0430'}
      targetMeta={plan.target_pct==null?'':money(plan.target_pct)+'% \u043e\u0442 \u0432\u0445\u043e\u0434\u0430'}/>
    <span>{'\u0427\u0438\u0441\u0442\u043e\u0435 RR:'} {plan.net_rr==null?'\u2014':money(plan.net_rr)+':1'} \u00b7 {'\u043c\u0438\u043d\u0438\u043c\u0443\u043c 1,5:1'}</span>
    <span>{'\u0420\u0430\u0441\u0445\u043e\u0434\u044b \u043d\u0430 \u043a\u0440\u0443\u0433:'} {plan.costs_pct==null?'\u2014':money(plan.costs_pct)+'% \u043e\u0442 \u0446\u0435\u043d\u044b'}</span>
    {market&&<div className="market-context"><b>{'\u041a\u043e\u043d\u0442\u0435\u043a\u0441\u0442 \u0440\u044b\u043d\u043a\u0430 \u00b7 \u043e\u043f\u0438\u0441\u0430\u0442\u0435\u043b\u044c\u043d\u044b\u0435 \u043f\u0440\u0438\u0437\u043d\u0430\u043a\u0438'}</b>
      <span>{'\u0424\u0438\u0433\u0443\u0440\u0430:'} {plan.side==='long'?'\u043b\u043e\u043d\u0433':'\u0448\u043e\u0440\u0442'}</span>
      <span>{'\u0422\u0440\u0435\u043d\u0434 15\u043c:'} {trendName(market.trend_15m)} \u00b7 {market.trend_15m_as_of?date(market.trend_15m_as_of):'\u0432\u0440\u0435\u043c\u044f \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e'}</span>
      <span>{'\u0414\u0432\u0438\u0436\u0435\u043d\u0438\u0435 5\u043c:'} {trendName(market.momentum_5m)} \u00b7 {market.momentum_5m_as_of?date(market.momentum_5m_as_of):'\u0432\u0440\u0435\u043c\u044f \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u043e'}</span>
    </div>}
    {!!plan.warnings?.filter(w=>!plan.market_background?.warnings.includes(w)).length&&<div className="plan-warnings">{plan.warnings.filter(w=>!plan.market_background?.warnings.includes(w)).map(w=><strong key={w}>{w}</strong>)}</div>}
    <MarketBackgroundPanel background={plan.market_background}/>
    <ForecastStatisticsPanel statistics={plan.compatible_statistics} scope="Раннее наблюдение; ниже статистика подтверждённых сигналов 15м → 5м."/>
    {plan.reason!==plan.confirmation_reason&&<small>{plan.reason}</small>}
    <small>{'\u0420\u0430\u0441\u0447\u0451\u0442 \u043d\u0430'} {date(plan.as_of)} \u00b7 {'\u043a\u043e\u0442\u0438\u0440\u043e\u0432\u043a\u0430'} {plan.quote_source==='bybit_orderbook_mid'?'\u043f\u043e mid \u0441\u0442\u0430\u043a\u0430\u043d\u0430':plan.quote_source==='bybit_ticker'?'Bybit ticker':'\u043d/\u0434'} \u00b7 {plan.quote_age_ms==null?'\u043d/\u0434':(plan.quote_age_ms/1000).toFixed(1)+'\u0441'} \u00b7 {'\u0441\u0442\u0430\u043a\u0430\u043d'} {plan.book_age_ms==null?'\u043d/\u0434':(plan.book_age_ms/1000).toFixed(1)+'\u0441'}</small>
  </article>;
}

function MarketBackgroundPanel({background}:{background?:MarketBackground}){
  if(!background||background.status==='not_applicable')return null;
  const directionName=(value:string|null|undefined)=>value==='up'?'рост':value==='down'?'снижение':value==='mixed'?'нейтрально':'н/д';
  const qualityName=(value:string|undefined)=>({valid:'полные данные',warming_up:'мало истории',partial:'частичная история',stale:'устарело',invalid:'ошибка данных',unavailable:'недоступно'}[value??'unavailable']??'н/д');
  const pct=(value:number|null|undefined)=>value==null?'н/д':`${value>0?'+':''}${(value*100).toFixed(2)}%`;
  const strengthName=({strong:'сильная',moderate:'умеренная',weak:'слабая',unavailable:'не рассчитана'} as Record<string,string>)[background.btc_correlation.strength]??'не рассчитана';
  return <section className="market-background" aria-label="Фон монеты и биткоина">
    <div className="market-background-heading"><b>Фон монеты и BTC</b><span>Закрытые свечи · расчёт {date(background.as_of)}</span></div>
    <div className="market-background-periods"><div className="market-background-row market-background-labels"><span>Период</span><span>{background.symbol==='BTCUSDT'?'BTC':'Монета'}</span><span>BTC</span><span>Относительно BTC</span></div>
      {[3,10,30].map(days=>{const row=background.periods[String(days)];return <div className="market-background-row" key={days}><b>{days} дней</b><span>{row?`${directionName(row.direction)} · ${pct(row.instrument_return)}`:'н/д'}</span><span>{row?.btc_return==null?'н/д':`${directionName(row.btc_direction)} \u00b7 ${pct(row.btc_return)}`}</span><span>{row?.relative_return==null?'н/д':pct(row.relative_return)}</span></div>})}
    </div>
    <span>Связь с BTC: {strengthName} · r {background.btc_correlation.value==null?'н/д':background.btc_correlation.value.toFixed(2)} · {background.btc_correlation.bars} свечей</span>
    <span>Дневной ATR: {pct(background.daily_atr_pct)} · данные {qualityName(background.daily_atr_quality)}</span>
    {background.advisory&&<p>{background.advisory}</p>}
    {!background.advisory&&background.status!=='valid'&&<p>История за 3, 10 и 30 дней ещё собирается; направление по неполному окну не выводится.</p>}
    {!!background.warnings.length&&<div className="market-background-warnings">{background.warnings.map(w=><strong key={w}>{w}</strong>)}</div>}
    {background.bars_as_of&&<small>Последняя общая свеча 4ч: {date(background.bars_as_of)} · качество корреляции: {qualityName(background.btc_correlation.quality)}</small>}
  </section>;
}

function ForecastStatisticsPanel({statistics,scope}:{statistics?:ForecastStatistics;scope?:string}){
  if(!statistics)return null;
  const pct=(value:number|null|undefined)=>value==null?'—':`${(value*100).toFixed(1)}%`;
  const cohort=(value:any)=>`${value?.sample_size??0}/${value?.minimum_sample_size??statistics.minimum_sample_size}`;
  return <section className="forecast-statistics">
    <b>Наблюдаемые исходы похожих прогнозов</b>
    <small>{scope??'Те же фигура, направление, версия правил и тип сетапа.'} Горизонты отсчитываются от первого доступного полного 5м интервала. Доля цели — частота в этой выборке, не вероятность будущей сделки.</small>
    <div className="table-wrap"><table><thead><tr><th>Окно</th><th>Все монеты · n</th><th>Цель</th><th>Средний net R</th><th>Эта монета · n / цель</th><th>Неоднозначно / no fill / пропуск · консервативный R</th></tr></thead><tbody>{[5,15,30].map(h=>{const row=statistics.horizons[String(h)];const all=row?.all_symbols;const own=row?.this_symbol;return <tr key={h}><td>{h}м</td><td>{cohort(all)}</td><td>{pct(all?.target_rate)}</td><td>{all?.avg_net_r==null?'—':`${all.avg_net_r.toFixed(2)}R`}</td><td>{cohort(own)} / {pct(own?.target_rate)}</td><td>{all?.ambiguous_count??0} / {all?.no_fill_count??0} / {all?.data_missing_count??0} · {all?.ambiguous_conservative_avg_net_r==null?'—':`${all.ambiguous_conservative_avg_net_r.toFixed(2)}R`}</td></tr>})}</tbody></table></div>
    <small>{statistics.status==='ready'?'Для одного или нескольких окон набрана выборка от 100 завершённых наблюдений.':'Малая выборка: нужны 100 завершённых однозначных исходов на окно. Показаны только наблюдения.'} Пропуски данных, невозможные входы и неоднозначные касания считаются отдельно.</small>
  </section>;
}

function PriceLevels({entry,stop,target,entryMeta,stopMeta,targetMeta}:{entry:number|null;stop:number|null;target:number|null;entryMeta?:string;stopMeta?:string;targetMeta?:string}){
  return <div className="price-levels">
    <div className="price-level entry"><small>ВХОД</small><strong>{price(entry)}</strong>{entryMeta&&<small>{entryMeta}</small>}</div>
    <div className="price-level stop"><small>СТОП-ЛОСС</small><strong>{price(stop)}</strong>{stopMeta&&<small>{stopMeta}</small>}</div>
    <div className="price-level target"><small>ТЕЙК-ПРОФИТ</small><strong>{price(target)}</strong>{targetMeta&&<small>{targetMeta}</small>}</div>
  </div>;
}

function qualityLabel(value?:DataQuality){
  if(!value)return 'нет данных';
  return ({valid:'актуально',warming_up:'набор окна',partial:'частично',stale:'устарело',invalid:'ошибка данных',unavailable:'недоступно',unknown:'качество неизвестно'} as Record<string,string>)[value.status]??value.status;
}
function QualityBadge({value}:{value?:DataQuality}){
  return <span className={`data-quality ${value?.status??'unavailable'}`}>{qualityLabel(value)}{value?.age_ms!=null?` · ${(value.age_ms/1000).toFixed(1)}с`:''}{value?.coverage_ms!=null&&value.window_ms?` · ${Math.floor(value.coverage_ms/1000)}/${Math.floor(value.window_ms/1000)}с`:''}</span>;
}

function DecisionPanel({data}:{data:DecisionEnvelope|null}){
  const c:DecisionContext|undefined=data?.current;
  if(!c)return <section className="panel decision"><div className="panel-heading"><h2>Контекст для решения</h2></div><p className="subtle">Выберите сигнал: сервис загрузит сохранённый снимок и текущие рыночные данные.</p></section>;
  const scenario:Record<string,string>={conditions_met_review_manually:'Условия сценария выполнены · оцените факты вручную',wait_for_retest:'Ждать подтверждения ретеста',rules_rejected:'Условия контекста не пройдены',manual_review_filters_unavailable:'Фигура и рынок доступны · часть фильтров отсутствует',insufficient_data:'Недостаточно данных',insufficient_rr:'Чистое RR ниже заданного минимума',late_entry:'Цена уже отошла от зоны пробоя',target_reached:'Проекция уже достигнута',scenario_invalidated:'Сценарий отменён',costs_unavailable:'Расходы не рассчитаны'};
  const pct=(x:number|null|undefined)=>x==null?'—':`${(x*100).toFixed(2)}%`;
  const fmt=(x:number|null|undefined)=>x==null?'—':money(x);
  const flow=(window:string)=>c.trade_flow[window];
  const timeframes=(c.quality?.timeframes as Record<string,DataQuality>|undefined)??{};
  const plan=c.entry_plans?.[0];
  const ten=c.orderbook.depth_by_band_bps?.['10'];
  const tenCoverage=c.orderbook.coverage_by_band_bps?.['10'];
  const liquidityLabel=(bps:string)=>{
    const depth=c.orderbook.depth_by_band_bps?.[bps];
    const coverage=c.orderbook.coverage_by_band_bps?.[bps];
    if(!depth)return `${bps} б.п.: нет снимка стакана`;
    return `${bps} б.п.: bid ${fmt(depth.bid_usdt)} / ask ${fmt(depth.ask_usdt)} USDT · покрытие ${coverage?.bid?'bid':'нет bid'} / ${coverage?.ask?'ask':'нет ask'}`;
  };
  return <section className="panel decision"><div className="panel-heading"><div><div className="eyebrow">РУЧНАЯ ОЦЕНКА · ПОКАЗАНЫ ФАКТЫ И ПРОТИВОРЕЧИЯ</div><h2>Контекст для решения</h2></div><span className="subtle">Текущая оценка {date(c.as_of)}</span></div>
    <div className="decision-status">{scenario[c.scenario]??c.scenario}</div>
    <div className="decision-freshness"><b>Свежесть обязательных данных</b><span>Цена <QualityBadge value={c.quality?.quote as DataQuality|undefined}/></span><span>1ч <QualityBadge value={timeframes['60']}/></span><span>15м <QualityBadge value={timeframes['15']}/></span><span>5м <QualityBadge value={timeframes['5']}/></span><span>Стакан <QualityBadge value={c.quality?.orderbook as DataQuality|undefined}/></span></div>
    {c.plain_summary&&<p className="decision-explanation">{c.plain_summary}</p>}
    <MarketBackgroundPanel background={c.market_background}/>
    <ForecastStatisticsPanel statistics={c.compatible_statistics}/>
    <div className="decision-grid">
      <div><b>Контекст · {c.instrument?.asset_class??c.asset_class??'актив'} · 1ч → 15м → 5м</b><span>Режим: {c.market_context?.scenario??'недостаточно данных'}</span><span>Направление 1ч: {c.market_context?.hourly_bias??'не определено'}</span><span>Структура 15м: {c.market_context?.structure_15m?.direction??'не определена'}</span><span>Ближайшее препятствие: {c.nearest_obstacle?`${c.nearest_obstacle.name} · ${price(c.nearest_obstacle.price)} · ${c.nearest_obstacle.distance_atr?.toFixed(2)??'—'} ATR`:'до проекции препятствие не найдено'}</span></div>
      <div><b>Подтверждение пробоя</b><span>Закрытие 5м: {c.confirmation_5m?.bar_time?date(c.confirmation_5m.bar_time):'нет нужной закрытой свечи'}</span><span>Закрытие / граница: {price(c.confirmation_5m?.close)} / {price(c.confirmation_5m?.boundary)} · расстояние {c.confirmation_5m?.distance_atr==null?'—':`${c.confirmation_5m.distance_atr.toFixed(2)} ATR`} (порог {c.confirmation_5m?.required_distance_atr==null?'—':`${c.confirmation_5m.required_distance_atr.toFixed(2)} ATR`})</span><span>Оборот: {fmt(c.volume_confirmation?.breakout_turnover_usdt)} / медиана {fmt(c.volume_confirmation?.median_turnover_previous_20_usdt)} USDT · {c.volume_confirmation?.multiple==null?'—':`${c.volume_confirmation.multiple.toFixed(2)}×`}</span><span>Условие отмены: {plan?.cancel_if??'исходный стоп или возврат внутрь фигуры'}</span></div>
      <div><b>Уровни и волатильность</b><span>SL {price(c.stop)} · проекция TP {price(c.target)}</span><span>Текущая цена {price(c.current_price)} · VWAP UTC {price(c.vwap_utc_5m)} ({c.distance_to_vwap_atr==null?'—':`${c.distance_to_vwap_atr.toFixed(2)} ATR`})</span><span>ATR {price(c.atr)} · ранг за 100 закрытых свечей {pct(c.atr_percentile_100)}</span><span>RR до проекции {c.net_rr_to_target==null?'н/д':`${fmt(c.net_rr_to_target)}:1`} · до препятствия {c.net_rr_to_obstacle==null?'—':`${fmt(c.net_rr_to_obstacle)}:1`}</span>{c.levels.slice(0,5).map(level=><span key={`${level.name}-${level.price}`}>{level.name}: {price(level.price)} · {level.distance_atr?.toFixed(2)??'—'} ATR</span>)}</div>
      <div><b>Ликвидность · видимая глубина</b><span>Bid / ask: {price(c.orderbook.best_bid)} / {price(c.orderbook.best_ask)} · спред {c.orderbook.spread_bps==null?'н/д':`${c.orderbook.spread_bps.toFixed(2)} б.п.`}</span>{['5','10','25'].map(bps=><span key={bps}>{liquidityLabel(bps)}</span>)}<span>Обновление стакана: {c.orderbook.age_ms==null?'нет данных':`${(c.orderbook.age_ms/1000).toFixed(1)}с назад`} · поколение {c.orderbook.generation??'—'}</span><small>Глубина описывает видимые заявки; она не оценивает исполнение личного объёма.</small></div>
      <div><b>Участие в движении · сделки USDT</b>{['1','5','15'].map(window=><span key={window}>{window}м: покупки {fmt(flow(window)?.buy_usdt)} · продажи {fmt(flow(window)?.sell_usdt)} · net {fmt(flow(window)?.net_usdt)} · <QualityBadge value={flow(window)?.quality}/></span>)}<span>OI: {pct(c.open_interest.change_5m)} / {pct(c.open_interest.change_15m)} / {pct(c.open_interest.change_1h)} за 5м / 15м / 1ч · стоимость {fmt(c.open_interest.value_usdt)} USDT</span></div>
      <div><b>Деривативы и фон</b><span>Funding {c.funding?.available?pct(c.funding.rate):'н/д'} · расчёт {c.funding?.next_time?date(c.funding.next_time):'—'} · <QualityBadge value={c.funding?.quality}/></span><span>Mark / index: {c.mark_index_basis_pct==null?'н/д':pct(c.mark_index_basis_pct)}</span><span>BTC: {c.btc_trend?.passed==null?'не определён':c.btc_trend.passed?'по направлению':'против направления'} · ширина группы {c.market_breadth?.up??0} вверх / {c.market_breadth?.down??0} вниз</span><span>Относительная сила к BTC 15м / 1ч / 24ч: {pct(c.relative_strength?.['15m'] as number|null|undefined)} / {pct(c.relative_strength?.['1h'] as number|null|undefined)} / {pct(c.relative_strength_24h)}</span><SpotFuturesPanel data={c.spot_futures}/></div>
      <div><b>Ликвидации, наблюдавшиеся в потоке</b>{(['5','15'] as const).map(window=>{const row=c.liquidations?.[window];return <span key={window}>{window}м: закрытие long {fmt(row?.long_liquidated_usdt)} · short {fmt(row?.short_liquidated_usdt)} USDT · событий {row?.count??0} · <QualityBadge value={row?.quality}/></span>})}<small>Цена события на бирже — bankruptcy price; наблюдавшийся поток может быть неполным.</small></div>
      <div><b>Условия и статистика</b><span>Комиссия: {c.fee_available?`${pct(c.fee_per_side)} на сторону`:'нет актуальной ставки'}</span><span>Допущения: комиссия Bybit и проскальзывание {pct(c.slippage_per_side)} на сторону; funding по текущей ставке.</span><span>Новости: источник не подключён · макрособытия: источник не подключён</span></div>
    </div>
    <details className="figure-plan-details"><summary>Сценарии входа, стоп и тейк</summary><EntryPlans plans={c.entry_plans}/></details>
    {!!c.supporting_factors.length&&<div className="decision-notes"><b>Поддерживает сценарий</b>{c.supporting_factors.map(item=><span key={item}>{item}</span>)}</div>}
    {!!c.risks.length&&<div className="decision-notes risk"><b>Противоречия и риски</b>{c.risks.map(item=><span key={item}>{item}</span>)}</div>}
    {!!c.unknown_data.length&&<div className="decision-notes unknown"><b>Данных пока нет</b>{c.unknown_data.map(item=><span key={item}>{item}</span>)}</div>}
    {data?.snapshot?<p className="subtle">Сохранённый снимок сигнала: {date(data.snapshot.as_of)} · текущая оценка выше на {date(c.as_of)}.</p>:<p className="subtle">Для этого сигнала нет сохранённого контекстного снимка. Сейчас показаны доступные текущие данные.</p>}
    <p className="subtle">OI сам по себе не задаёт направление. Стакан содержит видимые заявки, которые могут измениться или быть сняты.</p>
  </section>;
}

function SpotFuturesPanel({data}:{data?:SpotFuturesContext}){
  if(!data||data.status==='unavailable'||data.status==='unmatched')return <small>Спот / фьючерс: {data?.explanation??'данные недоступны'}</small>;
  const percent=(x:number|null|undefined)=>x==null?'н/д':`${x>0?'+':''}${x.toFixed(3)}%`;
  const band=(book?:BookMetrics)=>{
    const depth=book?.depth_by_band_bps?.['10'];const coverage=book?.coverage_by_band_bps?.['10'];
    return depth?`${money(depth.bid_usdt)} / ${money(depth.ask_usdt)} USDT · покрытие ${coverage?.bid?'bid':'нет bid'} / ${coverage?.ask?'ask':'нет ask'}`:'нет свежего стакана';
  };
  const imbalance=(book?:BookMetrics)=>book?.imbalance_pct==null?'частично / н/д':`${book.imbalance_pct>0?'+':''}${book.imbalance_pct.toFixed(1)}%`;
  return <div className="spot-futures"><div className="spot-futures-heading"><b>Спот / фьючерсы</b><span className={`status ${data.status==='ready'?'open':'rejected'}`}>{data.status==='ready'?'синхронизировано':data.status==='partial'?'частичное покрытие':'данные не синхронизированы'}</span></div>
    {data.spot&&data.perpetual&&<div className="spot-futures-grid"><div><b>Спот · {data.spot_symbol??'—'}</b><span>Спред {data.spot.spread_bps?.toFixed(2)??'н/д'} б.п. · глубина ±10 б.п. {band(data.spot)}</span><span>Дисбаланс {imbalance(data.spot)} · возраст {data.spot.age_ms==null?'—':`${(data.spot.age_ms/1000).toFixed(1)}с`}</span></div><div><b>Perpetual</b><span>Спред {data.perpetual.spread_bps?.toFixed(2)??'н/д'} б.п. · глубина ±10 б.п. {band(data.perpetual)}</span><span>Дисбаланс {imbalance(data.perpetual)} · возраст {data.perpetual.age_ms==null?'—':`${(data.perpetual.age_ms/1000).toFixed(1)}с`}</span></div></div>}
    {data.price_basis_pct!=null&&<div className="spot-futures-basis">Спот / perpetual basis {percent(data.price_basis_pct)} · разница дисбалансов {data.imbalance_difference_pp==null?'н/д':`${data.imbalance_difference_pp>0?'+':''}${data.imbalance_difference_pp.toFixed(1)} п.п.`}</div>}
    <small>{data.explanation??'Сравнение отражает только видимые заявки и цены.'}</small>
  </div>;
}

function PortfolioPanel({report,progress}:{report:PortfolioReport|null;progress:{stage:string;message:string}|null}){
  return <section className="panel"><div className="panel-heading"><div><div className="eyebrow">ОБЩИЙ ВИРТУАЛЬНЫЙ ПОРТФЕЛЬ</div><h2>60 дней · топ-20 пар · четыре таймфрейма</h2></div></div>
    <p className="subtle">{progress?.message||'Ожидание исторического прогона.'}</p>
    {report&&<><p className="subtle">Состав пар зафиксирован {date(report.manifest.captured_at)}. Период: {date(report.manifest.start)} — {date(report.manifest.end)}. {report.execution_model}.</p>
      <div className="metrics report">{[['Сигналов',report.total.signals],['Закрытых сделок',report.total.closed],['Прибыльных',`${money(report.total.win_rate*100)} %`],['Реализованный результат',`${money(report.total.realized_net)} USDT`],['Капитал с открытыми',`${money(report.total.equity)} USDT`],['Макс. просадка',`${money(report.total.max_drawdown*100)} %`]].map(([l,v])=><div className="metric" key={l}><span>{l}</span><strong>{v}</strong></div>)}</div>
      <p className="subtle">Открыто: {report.total.opened}; отклонено: {report.total.rejected}; открытых на конец: {report.total.open_positions.length}. Funding: {money(report.total.funding)} USDT. Доля прибыльных рассчитана только по закрытым сделкам.</p>
      <div className="table-wrap"><table><thead><tr><th>Фигура</th><th>Сигналы</th><th>Открыто</th><th>Отклонено</th><th>Закрыто</th><th>Прибыльных</th><th>Доля</th><th>Чистый результат</th></tr></thead><tbody>{report.patterns.map(p=><tr key={p.pattern}><td>{p.name}</td><td>{p.signals}</td><td>{p.opened}</td><td>{p.rejected}</td><td>{p.closed}</td><td>{p.wins}</td><td>{money(p.win_rate*100)} %</td><td className={p.net>=0?'positive':'negative'}>{money(p.net)} USDT</td></tr>)}</tbody></table></div>
      <h3>По таймфрейму</h3><div className="table-wrap"><table><thead><tr><th>ТФ</th><th>Сигналы</th><th>Открыто</th><th>Закрыто</th><th>Прибыльных</th><th>Доля</th><th>Результат</th></tr></thead><tbody>{Object.entries(report.timeframes).map(([tf,v])=><tr key={tf}><td>{tfName(tf)}</td><td>{v.signals}</td><td>{v.opened}</td><td>{v.closed}</td><td>{v.wins}</td><td>{money(v.win_rate*100)} %</td><td>{money(v.net)} USDT</td></tr>)}</tbody></table></div>
      <h3>По паре и покрытию</h3><div className="table-wrap"><table><thead><tr><th>Пара</th><th>Дней истории</th><th>Сигналы</th><th>Открыто</th><th>Закрыто</th><th>Прибыльных</th><th>Доля</th><th>Результат</th></tr></thead><tbody>{report.manifest.symbols.map(s=><tr key={s}><td>{s}</td><td>{money(report.coverage[s].days)}</td><td>{report.pairs[s].signals}</td><td>{report.pairs[s].opened}</td><td>{report.pairs[s].closed}</td><td>{report.pairs[s].wins}</td><td>{money(report.pairs[s].win_rate*100)} %</td><td>{money(report.pairs[s].net)} USDT</td></tr>)}</tbody></table></div>
      <h3>Причины отказов</h3><div className="table-wrap"><table><thead><tr><th>Причина</th><th>Количество</th></tr></thead><tbody>{Object.entries(report.rejection_reasons).map(([reason,count])=><tr key={reason}><td>{reason}</td><td>{count}</td></tr>)}</tbody></table></div>
      <div className="controls">{['summary.json','patterns.csv','timeframes.csv','pairs.csv','signals.csv','trades.csv','trades.json'].map(file=><a key={file} className="button" href={`/api/portfolio-backtest/file/${file}`} download={file}>{file}</a>)}</div>
      {report.total.open_positions.length>0&&<><h3>Незакрытые позиции</h3><TradeTable trades={report.total.open_positions}/></>}
    </>}
  </section>;
}

function ForecastAblationPanel({report}:{report:ForecastOutcomesReport|null}){
  if(!report)return <div className="forecast-research"><h3>Исходы прогнозов</h3><p className="subtle">Загружаю журнал новых подтверждённых сценариев.</p></div>;
  const names:Record<string,string>={hourly_context_pass:'Контекст 1ч / структура 15м',volume_pass:'Оборот пробоя 5м',coin_10d_supports_side:'Движение монеты за 10 дней',btc_10d_adverse_when_strongly_correlated:'BTC против стороны при сильной связи'};
  const status=(m:any)=>m?.sample_size>=m?.minimum_sample_size?'выборка набрана':'мало примеров';
  const target=(m:any)=>m?.target_rate==null?'—':`${(m.target_rate*100).toFixed(1)}%`;
  const r=(m:any)=>m?.avg_net_r==null?'—':`${m.avg_net_r.toFixed(2)}R`;
  return <div className="forecast-research"><div className="panel-heading"><div><div className="eyebrow">ЖИВЫЕ СНИМКИ · БЕЗ ИЗМЕНЕНИЯ ВХОДОВ</div><h3>Исходы подтверждённых прогнозов</h3></div><span>{report.candidate_count} наблюдений</span></div>
    <p className="subtle">{report.evaluation_model} Проверенные ранее исторические данные сюда не переносятся. {report.ablation.warning}</p>
    {[5,15,30].map(h=>{const item=report.ablation.horizons[String(h)];const all=item?.all_confirmed;return <div key={h}><b>Окно {h} минут · {all?.sample_size??0} однозначных исходов · {status(all)}</b><small>Неоднозначно {all?.ambiguous_count??0} · без входа {all?.no_fill_count??0} · нет данных {all?.data_missing_count??0} · средний результат при допущении «стоп первым» {all?.ambiguous_conservative_avg_net_r==null?'—':`${all.ambiguous_conservative_avg_net_r.toFixed(2)}R`}</small>
      <div className="table-wrap"><table><thead><tr><th>Фактор</th><th>Прошёл · n</th><th>Цель</th><th>Средний net R</th><th>Отсеяно целей</th><th>Отсеяно стопов</th></tr></thead><tbody>{Object.entries(item?.factors??{}).map(([key,value])=><tr key={key}><td>{names[key]??key}<small>{value.interpretation}</small></td><td>{value.passing.sample_size}</td><td>{target(value.passing)}</td><td>{r(value.passing)}</td><td>{value.excluded_targets}</td><td>{value.excluded_stops}</td></tr>)}</tbody></table></div>
    </div>})}
    <small>Неоднозначных, no fill и строк без данных программа считает отдельно. Доля цели описывает завершённые наблюдения в данном окне, она не является вероятностью будущей прибыли.</small>
  </div>;
}

function ResearchPanel({data,forecastOutcomes}:{data:{progress:{stage:string;message:string}|null;report:ResearchReport|null;approval?:{status:string;eligible?:boolean;message?:string}}|null;forecastOutcomes:ForecastOutcomesReport|null}){
  const [equity,setEquity]=useState<number[]>([]);
  const report=data?.report;const approval=data?.approval??report?.approval;const status=approval?.status??'not_run';
  const finalVariant=report?.selected??report?.variants?.find(v=>v.holdout);
  const hasHoldout=!!report?.variants?.some(v=>v.holdout);
  useEffect(()=>{if(!hasHoldout){setEquity([]);return}let active=true;fetch('/api/research/file/equity.csv').then(r=>r.ok?r.text():'').then(csv=>{if(!active||!csv)return;const values=csv.trim().split(/\r?\n/).slice(1).map(r=>Number(r.split(',')[1])).filter(Number.isFinite);setEquity(values)}).catch(()=>{});return()=>{active=false}},[hasHoldout,report?.approval?.status]);
  const minEquity=Math.min(...equity),maxEquity=Math.max(...equity),span=maxEquity-minEquity||1;
  const equityPoints=equity.map((v,i)=>`${equity.length<2?0:i*600/(equity.length-1)},${145-(v-minEquity)*130/span}`).join(' ');
  const statusText:Record<string,string>={replay_required:'\u041d\u0443\u0436\u043d\u0430 \u043d\u043e\u0432\u0430\u044f \u0438\u0441\u0442\u043e\u0440\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430',passed:'Допуск пройден',failed:'Проверка не пройдена',insufficient_data:'Недостаточно данных',not_run:'Историческая проверка ещё не завершена',invalid_report:'Отчёт не прошёл проверку',diagnostic_only:'Диагностический прогон · допуск закрыт'};
  return <section className="panel settings"><div className="panel-heading"><div><div className="eyebrow">ВЕРСИЯ FIGURES-CONTEXT-V4</div><h2>{statusText[status]??status}</h2></div><span className={`status ${status==='passed'?'open':'rejected'}`}>{status==='passed'?'допущена':statusText[status]??status}</span></div>
    <p className="subtle">{approval?.message??report?.message??data?.progress?.message??'Период: 05.08.2025 — 05.08.2026. Для входов нужны фильтр тренда старшего таймфрейма, объём пробоя и вариант, прошедший проверку на отложенной истории.'}</p>
    {report?.audit?.holdout_independent===false&&<div className="notice">Повторный holdout использует уже просмотренный период. Это диагностика исправления, а не независимое подтверждение прибыльности; автоматический допуск закрыт.</div>}
    {finalVariant&&<><p>{report?.selected?'Выбранный вариант':'Вариант, проверенный на holdout'}: <b>{finalVariant.entry==='retest'?'ретест':'сразу после пробоя'}</b>, чистое RR {finalVariant.net_rr}:1, временной выход {finalVariant.time_exit??'без ограничения'}.</p>{finalVariant.holdout&&<div className="metrics report">{[['Сделки holdout',finalVariant.holdout.closed],['Чистый результат',`${money(finalVariant.holdout.net)} USDT`],['Profit factor',money(finalVariant.holdout.profit_factor)],['Просадка',`${money((finalVariant.holdout.max_drawdown??0)*100)} %`],['Открытых на конец',finalVariant.holdout.open],['Удвоенное проскальзывание',`${money(finalVariant.stress_test_net)} USDT`]].map(([label,value])=><div className="metric" key={label}><span>{label}</span><strong>{value}</strong></div>)}</div>}{Array.isArray(finalVariant.holdout?.open_positions)&&finalVariant.holdout.open_positions.length>0&&<div className="notice">Открытые позиции на конец holdout: {finalVariant.holdout.open_positions.map((p:any)=><span key={p.id}>{p.symbol} {p.side.toUpperCase()} · вход {price(p.entry)} · SL {price(p.stop)} · TP {price(p.target)} · {price(p.qty)}; </span>)}</div>}</>}
    {equity.length>1&&<div className="equity-chart"><h3>Капитал выбранного варианта на итоговом тесте</h3><svg viewBox="0 0 600 160" role="img" aria-label="Изменение капитала на итоговом тесте"><line x1="0" y1="145" x2="600" y2="145"/><polyline points={equityPoints}/></svg><div className="chart-labels"><span>{money(minEquity)} USDT</span><span>{money(maxEquity)} USDT</span></div></div>}
    {report?.variants&&<>
      <div className="table-wrap"><table><thead><tr><th>{'\u0412\u0430\u0440\u0438\u0430\u043d\u0442'}</th><th>{'\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 validation'}</th><th>{'\u0421\u0434\u0435\u043b\u043a\u0438'}</th><th>{'\u0427\u0438\u0441\u0442\u044b\u0439 \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442'}</th><th>Profit factor</th><th>{'\u041f\u0440\u043e\u0441\u0430\u0434\u043a\u0430'}</th></tr></thead><tbody>{report.variants.map(v=><tr key={v.id}><td>{v.id}</td><td>{v.approval??(v.id===finalVariant?.id?"\u0432\u044b\u0431\u0440\u0430\u043d \u0434\u043b\u044f holdout":"\u043d\u0435 \u0432\u044b\u0431\u0440\u0430\u043d")}</td><td>{v.validation?.closed??'\u2014'}</td><td>{v.validation?.net?.toFixed(2)??'\u2014'} USDT</td><td>{v.validation?.profit_factor?.toFixed(2)??'\u2014'}</td><td>{v.validation?.max_drawdown==null?'\u2014':`${(v.validation.max_drawdown*100).toFixed(2)} %`}</td></tr>)}</tbody></table></div>
      <h3>{'\u0412\u0430\u043b\u0438\u0434\u0430\u0446\u0438\u044f \u043f\u043e \u0441\u0446\u0435\u043d\u0430\u0440\u0438\u044f\u043c'}</h3>
      <div className="table-wrap"><table><thead><tr><th>{'\u0412\u0430\u0440\u0438\u0430\u043d\u0442'}</th><th>{'\u0421\u0446\u0435\u043d\u0430\u0440\u0438\u0439'}</th><th>{'\u0421\u0434\u0435\u043b\u043a\u0438'}</th><th>{'\u0414\u043e\u043b\u044f \u043f\u0440\u0438\u0431\u044b\u043b\u044c\u043d\u044b\u0445'}</th><th>{'\u0427\u0438\u0441\u0442\u044b\u0439 \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442'}</th><th>Profit factor</th></tr></thead><tbody>{report.variants.flatMap(v=>['continuation','reversal'].map(scenario=>{const m=v.validation?.by_market_scenario?.[scenario];return <tr key={`${v.id}-${scenario}`}><td>{v.id}</td><td>{scenario==='continuation'?'\u041f\u0440\u043e\u0434\u043e\u043b\u0436\u0435\u043d\u0438\u0435':'\u0420\u0430\u0437\u0432\u043e\u0440\u043e\u0442'}</td><td>{m?.closed??'\u2014'}</td><td>{m?`${money(m.win_rate*100)} %`:'\u2014'}</td><td>{m?`${money(m.net)} USDT`:'\u2014'}</td><td>{m?money(m.profit_factor):'\u2014'}</td></tr>}))}</tbody></table></div>
    </>}
    {report?.holdout_signals&&<>
      <h3>{'\u041f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0451\u043d\u043d\u044b\u0435 \u0444\u0438\u0433\u0443\u0440\u044b \u0432 holdout'}: {report.holdout_signals.confirmed}</h3>
      <p className="subtle">{'\u041e\u0442\u043a\u0430\u0437 \u043f\u043e \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442\u0443'}: {report.holdout_signals.setup_rejected??0} | {'\u0411\u0435\u0437 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f 5m'}: {report.holdout_signals.confirmation_rejected??0} | {'\u041f\u043e \u043e\u0431\u043e\u0440\u043e\u0442\u0443'}: {report.holdout_signals.volume_rejected}</p>
      <div className="table-wrap"><table><thead><tr><th>{'\u0424\u0438\u0433\u0443\u0440\u0430'}</th><th>{'\u0421\u0438\u0433\u043d\u0430\u043b\u044b'}</th><th>{'\u041e\u0442\u043a\u0430\u0437 \u043f\u043e \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442\u0443'}</th><th>{'\u041d\u0435\u0442 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f 5m'}</th><th>{'\u041e\u0442\u043a\u0430\u0437 \u043f\u043e \u043e\u0431\u043e\u0440\u043e\u0442\u0443'}</th></tr></thead><tbody>{report.holdout_signals.by_pattern.map(p=><tr key={p.pattern}><td>{p.name}</td><td>{p.signals}</td><td>{p.setup_rejected??0}</td><td>{p.confirmation_rejected??0}</td><td>{p.volume_rejected}</td></tr>)}</tbody></table></div>
      {report.holdout_signals.by_market_scenario&&<><h3>{'Holdout \u043f\u043e \u0442\u0438\u043f\u0443 \u0441\u0435\u0442\u0430\u043f\u0430'}</h3><div className="table-wrap"><table><thead><tr><th>{'\u0421\u0446\u0435\u043d\u0430\u0440\u0438\u0439'}</th><th>{'\u0421\u0438\u0433\u043d\u0430\u043b\u044b'}</th><th>{'\u0414\u043e\u043f\u0443\u0449\u0435\u043d\u043e'}</th><th>{'\u0421\u0434\u0435\u043b\u043a\u0438'}</th><th>{'\u0414\u043e\u043b\u044f \u043f\u0440\u0438\u0431\u044b\u043b\u044c\u043d\u044b\u0445'}</th><th>{'\u0427\u0438\u0441\u0442\u044b\u0439 \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442'}</th></tr></thead><tbody>{report.holdout_signals.by_market_scenario.map(row=><tr key={row.scenario}><td>{row.scenario==='continuation'?'\u041f\u0440\u043e\u0434\u043e\u043b\u0436\u0435\u043d\u0438\u0435':row.scenario==='reversal'?'\u0420\u0430\u0437\u0432\u043e\u0440\u043e\u0442':'\u041e\u0436\u0438\u0434\u0430\u043d\u0438\u0435 \u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f'}</td><td>{row.signals}</td><td>{row.eligible}</td><td>{row.trades}</td><td>{row.trades?`${money(row.wins/row.trades*100)} %`:'\u2014'}</td><td>{money(row.net)} USDT</td></tr>)}</tbody></table></div></>}
    </>}
    {report?.coverage&&<p className="subtle">Покрытие пар: {Object.entries(report.coverage).map(([s,c])=>`${s} ${c.days.toFixed(0)}д`).join(' · ')}</p>}
    <ForecastAblationPanel report={forecastOutcomes}/>
    <div className="controls">{['summary.json','variants.csv','signals.csv','signals_by_pattern.csv','trades.csv','equity.csv','coverage.json'].map(file=><a key={file} className="button" href={`/api/research/file/${file}`} download={file}>{file}</a>)}</div>
    {!report&&<div className="empty"><h3>{data?.progress?.message??'Ожидание отчёта исследования'}</h3><p>Автоматические виртуальные входы выключены до прохождения критериев допуска.</p></div>}
  </section>;
}

function AccountPanel({data,review,setups,selectExecution,linkExecution}:{data:AccountFeed|null;review:AccountReview|null;setups:any[];selectExecution:(execution:AccountExecution)=>void;linkExecution:(execId:string,setupId:string)=>Promise<void>}){
  const [linkId,setLinkId]=useState('');
  useEffect(()=>setLinkId(review?.setup?.id??''),[review?.execution.execId,review?.setup?.id]);
  const status=data?.status;
  const phaseLabel:Record<string,string>={not_configured:'\u041d\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d\u043e',waiting:'\u041e\u0436\u0438\u0434\u0430\u043d\u0438\u0435',verifying:'\u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043a\u043b\u044e\u0447\u0430',syncing:'\u0421\u0432\u0435\u0440\u043a\u0430 \u0434\u0430\u043d\u043d\u044b\u0445',live:'\u041f\u043e\u0442\u043e\u043a \u0430\u043a\u0442\u0438\u0432\u0435\u043d',disconnected:'\u041e\u0442\u043a\u043b\u044e\u0447\u0435\u043d\u043e',read_only_required:'\u041d\u0443\u0436\u0435\u043d \u043a\u043b\u044e\u0447 \u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f \u0447\u0442\u0435\u043d\u0438\u044f',permissions_missing:'\u041d\u0435\u0442 \u043f\u0440\u0430\u0432 \u0447\u0442\u0435\u043d\u0438\u044f \u043f\u043e\u0437\u0438\u0446\u0438\u0439 \u0438 \u0437\u0430\u044f\u0432\u043e\u043a'};
  const num=(v:any)=>v==null||v===''?null:Number(v);
  const show=(v:any,digits=4)=>{const n=num(v);return n==null||!Number.isFinite(n)?'\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e':decimal(n,digits,digits)};
  const side=(v:string)=>v==='Buy'?'\u041f\u043e\u043a\u0443\u043f\u043a\u0430 / long':v==='Sell'?'\u041f\u0440\u043e\u0434\u0430\u0436\u0430 / short':v||'\u2014';
  const candidates=(review?.candidates??[]).map(x=>x.setup);
  const choices=[...new Map([...candidates,...setups.filter(x=>x.side===({Buy:'long',Sell:'short'} as Record<string,string>)[review?.execution?.side??''])].filter(Boolean).map(x=>[x.id,x])).values()];
  const fee=(x:any)=>x.execFee==null?'\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e':show(x.execFee,8)+' USDT';
  return <div className="account-page">
    <section className="panel account-status-panel"><div className="panel-heading"><div><div className="eyebrow">BYBIT PRIVATE DATA \u00b7 READ ONLY</div><h2>{'\u0421\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f'}</h2></div><span className={'account-connection '+(status?.connected?'ok':'')}>{status?.connected?'\u041f\u043e\u0442\u043e\u043a \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d':phaseLabel[status?.phase??'']??'\u041e\u0436\u0438\u0434\u0430\u043d\u0438\u0435'}</span></div>
      {!data&&<p className="subtle">{'\u0417\u0430\u0433\u0440\u0443\u0436\u0430\u0435\u043c \u0441\u0442\u0430\u0442\u0443\u0441 \u0438 \u0441\u0434\u0435\u043b\u043a\u0438 \u0441\u0447\u0451\u0442\u0430\u2026'}</p>}
      {status?.phase==='not_configured'&&<div className="account-notice"><strong>{'\u041c\u043e\u043d\u0438\u0442\u043e\u0440\u0438\u043d\u0433 \u043d\u0435 \u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043d'}</strong><span>{'\u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0432 \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u044b\u0439 .env \u043a\u043b\u044e\u0447\u0438 BYBIT_MONITOR_API_KEY \u0438 BYBIT_MONITOR_API_SECRET \u0441 \u043f\u0440\u0430\u0432\u043e\u043c read-only \u0438 \u043f\u0435\u0440\u0435\u0437\u0430\u043f\u0443\u0441\u0442\u0438\u0442\u0435 \u0441\u0435\u0440\u0432\u0438\u0441. \u041d\u0435 \u0432\u0441\u0442\u0430\u0432\u043b\u044f\u0439\u0442\u0435 \u043a\u043b\u044e\u0447\u0438 \u0432 \u0447\u0430\u0442 \u0438\u043b\u0438 \u0438\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441.'}</span></div>}
      {status?.error&&status.phase!=='not_configured'&&<div className="account-notice"><strong>{phaseLabel[status.phase]??'\u041e\u0448\u0438\u0431\u043a\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f'}</strong><span>{status.error}</span></div>}
      {status?.read_only===true&&<p className="subtle">{'\u041f\u0440\u0430\u0432\u043e \u0442\u043e\u043b\u044c\u043a\u043e \u0434\u043b\u044f \u0447\u0442\u0435\u043d\u0438\u044f \u043f\u0440\u043e\u0432\u0435\u0440\u0435\u043d\u043e. \u0414\u0430\u043d\u043d\u044b\u0435 Bybit \u043c\u043e\u0433\u0443\u0442 \u0432\u043a\u043b\u044e\u0447\u0430\u0442\u044c \u0441\u0434\u0435\u043b\u043a\u0438, \u0441\u043e\u0432\u0435\u0440\u0448\u0451\u043d\u043d\u044b\u0435 \u0447\u0435\u0440\u0435\u0437 Tiger.Trade; \u043a\u043b\u0438\u043a\u0438 \u0438 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044f \u0442\u043e\u043b\u044c\u043a\u043e \u0432 \u0442\u0435\u0440\u043c\u0438\u043d\u0430\u043b\u0435 \u0431\u0438\u0440\u0436\u0430 \u043d\u0435 \u043f\u0435\u0440\u0435\u0434\u0430\u0451\u0442.'}</p>}
      {status?.last_sync&&<small className="subtle">{'\u0421\u0432\u0435\u0440\u043a\u0430 REST:'} {date(status.last_sync)} \u00b7 {'\u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0435\u0435 \u0441\u043e\u0431\u044b\u0442\u0438\u0435:'} {status.last_message?date(status.last_message):'\u043e\u0436\u0438\u0434\u0430\u043d\u0438\u0435'}</small>}
    </section>
    <section className="panel account-panel"><div className="panel-heading"><div><div className="eyebrow">LINEAR \u00b7 USDT</div><h2>{'\u041e\u0442\u043a\u0440\u044b\u0442\u044b\u0435 \u043f\u043e\u0437\u0438\u0446\u0438\u0438'} <span className="count">{data?.positions.length??0}</span></h2></div></div>
      <div className="account-position-grid">{(data?.positions??[]).map((p,i)=><article className="account-position" key={p.symbol+'-'+p.positionIdx+'-'+i}>
        <div className="panel-heading"><b>{p.symbol}</b><span className={p.side==='Buy'?'positive':'negative'}>{side(p.side)}{Number(p.positionIdx)>0?' \u00b7 hedge '+p.positionIdx:''}</span></div>
        <span>{'\u0421\u0440\u0435\u0434\u043d\u044f\u044f'} {show(p.avgPrice)} \u00b7 {'\u043e\u0431\u044a\u0451\u043c'} {show(p.size,6)} \u00b7 {'\u043f\u043b\u0435\u0447\u043e'} {show(p.leverage,2)}</span>
        <span>{'uPnL'} {show(p.unrealisedPnl)} USDT \u00b7 {'\u043c\u0430\u0440\u043a\u0435\u0442-\u0446\u0435\u043d\u0430'} {show(p.markPrice)}</span>
        <div className="account-levels"><span className="level-stop">SL {show(p.stopLoss)}</span><span className="level-target">TP {show(p.takeProfit)}</span></div>
        <small>{'\u041e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u043e'} {p.updatedTime?date(Number(p.updatedTime)):data?.updated_at?date(data.updated_at):'\u2014'}</small>
      </article>)}</div>{!data?.positions.length&&<div className="empty"><p>{'\u041e\u0442\u043a\u0440\u044b\u0442\u044b\u0445 \u043f\u043e\u0437\u0438\u0446\u0438\u0439 \u043d\u0435\u0442 \u0438\u043b\u0438 \u0441\u0447\u0451\u0442 \u0435\u0449\u0451 \u043d\u0435 \u0441\u0432\u0435\u0440\u0435\u043d.'}</p></div>}
    </section>
    <section className="panel account-panel"><div className="panel-heading"><div><div className="eyebrow">BYBIT EXECUTIONS</div><h2>{'\u0418\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f'} <span className="count">{data?.executions.length??0}</span></h2></div><span className="subtle">{'\u041a\u043e\u043c\u0438\u0441\u0441\u0438\u044f, PnL \u0438\u043b\u0438 funding \u043c\u043e\u0433\u0443\u0442 \u0431\u044b\u0442\u044c \u043d\u0435\u043f\u043e\u043b\u043d\u044b\u043c\u0438 \u0432 \u0438\u0441\u0442\u043e\u0440\u0438\u0438.'}</span></div>
      <div className="table-wrap"><table><thead><tr><th>{'\u0412\u0440\u0435\u043c\u044f \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f'}</th><th>{'\u041f\u0430\u0440\u0430 / \u0441\u0442\u043e\u0440\u043e\u043d\u0430'}</th><th>{'\u0412\u0438\u0434 \u0438 \u0446\u0435\u043d\u0430'}</th><th>{'\u041e\u0431\u044a\u0451\u043c'}</th><th>{'\u041a\u043e\u043c\u0438\u0441\u0441\u0438\u044f'}</th><th>{'PnL \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f'}</th><th>{'\u0421\u0446\u0435\u043d\u0430\u0440\u0438\u0439'}</th></tr></thead><tbody>{(data?.executions??[]).map(x=><tr key={x.execId} className="account-execution-row" onClick={()=>selectExecution(x)}>
        <td>{date(Number(x.execTime))}</td><td><b>{x.symbol}</b><small>{side(x.side)} \u00b7 {Number(x.positionIdx)>0?'hedge '+x.positionIdx:'one-way'}</small></td>
        <td>{x.closedSize&&Number(x.closedSize)>0?'\u0427\u0430\u0441\u0442\u0438\u0447\u043d\u043e\u0435 \u0437\u0430\u043a\u0440\u044b\u0442\u0438\u0435 / \u0441\u043e\u043a\u0440\u0430\u0449\u0435\u043d\u0438\u0435':'\u0412\u0445\u043e\u0434 / \u0434\u043e\u0431\u043e\u0440'}<small>{price(num(x.execPrice))}</small></td>
        <td>{show(x.execQty,6)}</td><td>{fee(x)}</td><td>{x.execPnl==null?'\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e':show(x.execPnl)+' USDT'}</td>
        <td>{x.setup_id?'\u043f\u0440\u0438\u0432\u044f\u0437\u0430\u043d':x.closedSize&&Number(x.closedSize)>0?'\u0437\u0430\u043a\u0440\u044b\u0442\u0438\u0435': '\u043d\u0435 \u0441\u0432\u044f\u0437\u0430\u043d'}</td>
      </tr>)}</tbody></table>{!data?.executions.length&&<div className="empty"><p>{'\u0418\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u0439 \u0432 \u0437\u0430\u0433\u0440\u0443\u0436\u0435\u043d\u043d\u043e\u0439 \u0438\u0441\u0442\u043e\u0440\u0438\u0438 \u043f\u043e\u043a\u0430 \u043d\u0435\u0442.'}</p></div>}</div>
    </section>
    {!!data?.orders.length&&<section className="panel account-panel"><div className="panel-heading"><div><div className="eyebrow">BYBIT ORDERS</div><h2>{'\u0417\u0430\u044f\u0432\u043a\u0438'} <span className="count">{data.orders.length}</span></h2></div></div><div className="table-wrap"><table><thead><tr><th>{'\u041f\u0430\u0440\u0430'}</th><th>{'\u0421\u0442\u043e\u0440\u043e\u043d\u0430 / \u0442\u0438\u043f'}</th><th>{'\u0446\u0435\u043d\u0430 / \u0441\u0442\u043e\u043f'}</th><th>{'\u041e\u0431\u044a\u0451\u043c \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d'}</th><th>{'\u0421\u0442\u0430\u0442\u0443\u0441'}</th></tr></thead><tbody>{data.orders.slice(0,50).map(o=><tr key={o.orderId}><td>{o.symbol}</td><td>{side(o.side)}<small>{o.orderType}</small></td><td>{show(o.price)}<small>{o.triggerPrice&&Number(o.triggerPrice)>0?'\u0442\u0440\u0438\u0433\u0433\u0435\u0440 '+show(o.triggerPrice):'\u2014'}</small></td><td>{show(o.cumExecQty,6)} / {show(o.qty,6)}</td><td>{o.orderStatus}</td></tr>)}</tbody></table></div></section>}
    {review&&<section className="panel account-review"><div className="panel-heading"><div><div className="eyebrow">{'\u0420\u0410\u0417\u0411\u041e\u0420 \u0418\u0421\u041f\u041e\u041b\u041d\u0415\u041d\u0418\u042f'}</div><h2>{review.execution.symbol} \u00b7 {side(review.execution.side)} \u00b7 {date(Number(review.execution.execTime))}</h2></div></div>
      <div className={'decision-status '+(review.status==='captured'?'':'attention')}>{review.status==='captured'?'\u0421\u0442\u0430\u0442\u0443\u0441 \u0441\u0438\u0433\u043d\u0430\u043b\u0430 \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d \u043f\u0435\u0440\u0435\u0434 \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u0435\u043c':review.status==='stale'?'\u041e\u0446\u0435\u043d\u043a\u0430 \u0441\u0442\u0430\u0440\u0448\u0435 5 \u0441\u0435\u043a\u0443\u043d\u0434': '\u0421\u0442\u0430\u0442\u0443\u0441 \u043d\u0430 \u043c\u043e\u043c\u0435\u043d\u0442 \u0432\u0445\u043e\u0434\u0430 \u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u0435\u043d'}</div>
      <div className="account-review-grid"><div><b>{'\u0426\u0435\u043d\u0430 \u0438 \u043e\u0431\u044a\u0451\u043c \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f'}</b><span>{'\u0421\u0442\u043e\u0440\u043e\u043d\u0430'}: {side(review.execution.side)} \u00b7 positionIdx {review.execution.positionIdx??0}</span><span>{'\u0426\u0435\u043d\u0430'}: {price(num(review.execution.execPrice))}</span><span>{'\u041e\u0431\u044a\u0451\u043c'}: {show(review.execution.execQty,6)}</span><span>{'\u041a\u043e\u043c\u0438\u0441\u0441\u0438\u044f'}: {fee(review.execution)}</span><span>{'PnL \u0438\u0441\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f'}: {review.execution.execPnl==null?'\u043d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u043e':show(review.execution.execPnl)+' USDT'}</span></div>
        <div><b>{'\u0421\u0446\u0435\u043d\u0430\u0440\u0438\u0439 \u0438 \u0431\u0438\u0440\u0436\u0435\u0432\u044b\u0435 \u0443\u0440\u043e\u0432\u043d\u0438'}</b>{review.setup?<span>{names[review.setup.pattern]||review.setup.pattern} \u00b7 {review.setup.side} \u00b7 {tfName(review.setup.timeframe)} \u00b7 {review.assessment?.as_of?date(review.assessment.as_of):'\u2014'}</span>:<span>{'\u041d\u0435\u0442 \u043e\u0434\u043d\u043e\u0437\u043d\u0430\u0447\u043d\u043e\u0439 \u0441\u0432\u044f\u0437\u0438; \u0432\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0441\u043e\u043e\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u0443\u044e\u0449\u0438\u0439 \u0441\u0446\u0435\u043d\u0430\u0440\u0438\u0439 \u0432\u0440\u0443\u0447\u043d\u0443\u044e.'}</span>}
          <span>{'\u0411\u0438\u0440\u0436\u0435\u0432\u043e\u0439 SL:'} {review.position?.stopLoss&&Number(review.position.stopLoss)>0?price(num(review.position.stopLoss)):'\u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d'}</span><span>{'\u0411\u0438\u0440\u0436\u0435\u0432\u043e\u0439 TP:'} {review.position?.takeProfit&&Number(review.position.takeProfit)>0?price(num(review.position.takeProfit)):'\u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d'}</span>
        </div></div>
      {!!review.warnings.length&&<div className="plan-warnings">{review.warnings.map(w=><strong key={w}>{w}</strong>)}</div>}
      {!!choices.length&&<div className="account-link-form"><label>{'\u041f\u0440\u0438\u0432\u044f\u0437\u0430\u0442\u044c \u043a \u0441\u0446\u0435\u043d\u0430\u0440\u0438\u044e \u0442\u043e\u0439 \u0436\u0435 \u043f\u0430\u0440\u044b \u0438 \u0441\u0442\u043e\u0440\u043e\u043d\u044b'}<select value={linkId} onChange={e=>setLinkId(e.target.value)}><option value="">{'\u041d\u0435 \u043f\u0440\u0438\u0432\u044f\u0437\u044b\u0432\u0430\u0442\u044c'}</option>{choices.map(x=><option key={x.id} value={x.id}>{names[x.pattern]||x.pattern} \u00b7 {date(x.event_at??x.detected_at)} \u00b7 {x.side}</option>)}</select></label><button className="button" disabled={!linkId||linkId===review.setup?.id} onClick={()=>void linkExecution(review.execution.execId,linkId)}>{'\u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c \u043f\u0440\u0438\u0432\u044f\u0437\u043a\u0443'}</button></div>}
    </section>}
  </div>;
}

function TradeTable({trades}:{trades:Trade[]}){return <div className="table-wrap"><table><thead><tr><th>Монета</th><th>Фигура / ТФ</th><th>Вход &gt; выход</th><th>Объем</th><th>Комиссии</th><th>Funding</th><th>Чистый PnL</th><th>Время / статус</th></tr></thead><tbody>{trades.map(t=><tr key={t.id}><td><b>{t.symbol}</b><small className={t.side==='long'?'positive':'negative'}>{t.side.toUpperCase()}</small></td><td>{names[t.pattern]||t.pattern}<small>{tfName(t.timeframe)}</small>{t.strategy_version&&<small>{t.strategy_variant} · {t.entry_method}</small>}</td><td>{price(t.entry)} &gt; {price(t.exit)}<small>SL {price(t.stop)} · TP {price(t.target)}</small></td><td>{price(t.qty)}</td><td>{money(t.entry_fee+(t.exit_fee??0))}</td><td>{money(t.funding)}</td><td className={(t.net??0)>=0?'positive':'negative'}>{money(t.net)}</td><td>{date(t.closed??t.opened)}<small>{t.reason==='stop'?'Стоп':t.reason==='target'?'Тейк':t.reason==='time'?'По времени':'Открыта'}{t.recovered?' · восстановлено по свечам':''}</small></td></tr>)}</tbody></table>{!trades.length&&<div className="empty"><p>Сделок пока нет</p></div>}</div>}

createRoot(document.getElementById('root')!).render(<React.StrictMode><App/></React.StrictMode>);
