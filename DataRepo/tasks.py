import time
from django.http import JsonResponse, HttpResponse
from django.template import loader
import DataRepo.views as views
from DataRepo.compositeviews import BaseAdvancedSearchView

# from celery import shared_task
# from celery import Celery, shared_task

# celery = Celery('tasks', broker='amqp://guest@localhost//')
from TraceBase.celery import app

@app.task
def tsv_producer(response, iterator, headtmplt, rowtmplt, res, qry, dt, progress_observer):
    total_work_to_do = res.count() + 1
    i = 0
    for yielded in iterator(rowtmplt, headtmplt, res, qry, dt):
        i += 1
        response.writelines(yielded)
        print(f"Write yeild {i}")
        progress_observer.set_progress(i, total_work_to_do)
    # return response

@app.task(bind=True)
def tsv_producer2(self, response, iterator, headtmplt, rowtmplt, res, qry, dt):
    total_work_to_do = res.count() + 1
    i = 0
    for yielded in iterator(rowtmplt, headtmplt, res, qry, dt):
        i += 1
        response.writelines(yielded)
        tsv_producer2.update_state(state='COMPILING_DATA', meta={'current': i, 'total': total_work_to_do})
        print(f"Write yeild {i}")
    return JsonResponse({'current': total_work_to_do, 'total': total_work_to_do, 'output': response})
    # return response

@app.task(bind=True)
def tsv_producer3(self, filename, header_template, row_template, qry, dt):
    headtmplt = loader.get_template(header_template)
    rowtmplt = loader.get_template(row_template)
    basv = BaseAdvancedSearchView()
    if views.isValidQryObjPopulated(qry):
        q_exp = views.constructAdvancedQuery(qry)
        res, tot = views.performQuery(q_exp, qry["selectedtemplate"], basv)
    else:
        res, tot = views.getAllBrowseData(qry["selectedtemplate"], basv)
    response = HttpResponse(content='', content_type="application/text", status=200, reason=None, charset='utf-8')
    response["Content-Disposition"] = f"attachment; filename={filename}"
    total_work_to_do = res.count() + 1
    i = 1
    response.writelines(headtmplt.render({"qry": qry, "dt": dt}))
    tsv_producer3.update_state(state='COMPILING_DATA', meta={'current': i, 'total': total_work_to_do})
    for row in res:
        i += 1
        response.writelines(rowtmplt.render({"qry": qry, "row": row}))
        tsv_producer3.update_state(state='COMPILING_DATA', meta={'current': i, 'total': total_work_to_do})
        print(f"Write yeild {i}")
    return {'current': total_work_to_do, 'total': total_work_to_do}

@app.task(bind=True)
def loop(self, l):
    "simulate a long-running task like export of data or generateing a report"
    for i in range(int(l)):
        print(i)
        time.sleep(1)
        loop.update_state(state='PROGRESS',
                          meta={'current': i, 'total': l})
    print('Task completed')
    return {'current': 100, 'total': 100, }
