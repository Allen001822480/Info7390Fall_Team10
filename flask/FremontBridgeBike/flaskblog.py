from flask import Flask, render_template, url_for, flash, redirect
from forms import LoginForm
from prediction.final import get_input
from prediction.final import feature_engineering
from prediction.final import east_prediction
from prediction.final import west_prediction
import numpy as np

from flask import Response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import random
import io


app = Flask(__name__)

app.config['SECRET_KEY'] = '0c1a9a46ee1d7c9f705568283d55e61b'


@app.route("/", methods=['GET', 'POST'])
@app.route("/home", methods=['GET', 'POST'])
def home():
    form = LoginForm()
    if form.validate_on_submit():
        fig = create_figure(form.email.data)
        print(form.email.data + "lol")
        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        return Response(output.getvalue(), mimetype='image/png')
        # return redirect(url_for('result'))
    else:
        print("Invalid")
    return render_template('home.html', title='Home', form=form)


@app.route("/result")
def result():
    return render_template('result.html', title='result')


# @app.route('/plot')
# def plot_png():
#     fig = create_figure()
#     output = io.BytesIO()
#     FigureCanvas(fig).print_png(output)
#     return Response(output.getvalue(), mimetype='image/png')


def create_figure(day):
    df_in = get_input(int(day))
    df_x = feature_engineering(df_in)

    east_result = east_prediction(df_x)
    west_result = west_prediction(df_x)

    fig = Figure(figsize=(20, 12), dpi=100)

    e_ax = fig.add_subplot(2, 1, 1, autoscale_on=True, title='East Side')
    e_ax.grid(which='major', axis='both', linestyle='--')
    hour = np.arange(0, 24, 1)

    fig.suptitle('Fremont Bike Flow Prediction Day:' + day)
    e_ax.plot(hour, east_result)

    w_ax = fig.add_subplot(2, 1, 2, autoscale_on=True, title='West Side')
    w_ax.grid(which='major', axis='both', linestyle='--')
    w_ax.plot(hour, west_result)

    return fig


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=2000)
