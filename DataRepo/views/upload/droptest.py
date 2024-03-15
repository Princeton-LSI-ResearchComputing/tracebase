from django.views.generic.edit import FormView

from DataRepo.forms import DropTestListForm


# TODO: TEMPORARY CLASS FOR A QUICK PROTOTYPE TEST
class DropTestView(FormView):
    form_class = DropTestListForm
    template_name = "DataRepo/droptest.html"
    success_url = ""  # Same as template?

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)

        self.mzxml_files = request.FILES.getlist("mzxml_file_list")

        print(self.mzxml_files)

        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)

    def form_valid(self, form):

        return self.render_to_response(
            self.get_context_data(
                results=self.mzxml_files,
                form=form,
            )
        )
