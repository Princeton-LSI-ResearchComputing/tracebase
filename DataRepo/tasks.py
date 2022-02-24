import time
from django.template import loader
import DataRepo.views as views
from DataRepo.compositeviews import BaseAdvancedSearchView
from django.core.serializers import serialize

from TraceBase.celery import app

# Binding gives us access to the AsyncResult "self" object to issue status updates
@app.task(bind=True)
def tsv_producer(self, filename, header_template, row_template, qry, dt):
    # headtmplt, rowtmplt, basv, all methods, and response are all NOT json serializable.  All inputs into this "task"
    # need to be json serializable in order for the task to be saved and monitored in celery/rabbitmq/asyncresult, so
    # instead of passing those things in, they are created here using inputs that *are* json serializable.

    # Templates
    headtmplt = loader.get_template(header_template)
    rowtmplt = loader.get_template(row_template)

    # BaseAdvancedSearchView object
    basv = BaseAdvancedSearchView()

    # Execute the query
    if views.isValidQryObjPopulated(qry):
        q_exp = views.constructAdvancedQuery(qry)
        res, tot = views.performQuery(q_exp, qry["selectedtemplate"], basv)
    else:
        res, tot = views.getAllBrowseData(qry["selectedtemplate"], basv)
    
    # Initialize a response object needed to build the download file
    # response = HttpResponse(content='', content_type="application/text", status=200, reason=None, charset='utf-8')
    # response["Content-Disposition"] = f"attachment; filename={filename}"

    # Prepare the running status
    total_work_to_do = tot + 1
    i = 1

    # Keep the status updates to under 1000 to avoid exception: celery.backends.rpc.BacklogLimitExceeded
    throttled_work_to_do = total_work_to_do
    mag_shift = 1
    while throttled_work_to_do > 1000:
        mag_shift *= 10
        throttled_work_to_do = int(total_work_to_do / mag_shift) + 1

    # Build the download data
    output = headtmplt.render({"qry": qry, "dt": dt})
    throttled_i = 0
    tsv_producer.update_state(state='COMPILING_DATA', meta={'current': throttled_i, 'total': throttled_work_to_do})
    for row in res:
        i += 1
        output += rowtmplt.render({"qry": qry, "row": row})
        throttled_i = int(i / mag_shift)
        if i % mag_shift == 0:
            tsv_producer.update_state(state='COMPILING_DATA', meta={'current': throttled_i, 'total': throttled_work_to_do})

    tsv_producer.update_state(state='SUCCESS', meta={'current': throttled_work_to_do, 'total': throttled_work_to_do})

    print("Returning response ", output)
    data = {"output": output, "filename": filename}
    # Return success
    return data

# Sanity check (for debugging)
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
