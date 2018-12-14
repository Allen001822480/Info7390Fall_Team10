from flask import Flask, render_template, url_for, flash, redirect
from forms import LoginForm
from prediction.final import getPrediction
from multiprocessing import Process
app = Flask(__name__)

app.config['SECRET_KEY'] = '0c1a9a46ee1d7c9f705568283d55e61b'


@app.route("/", methods=['GET', 'POST'])
@app.route("/home", methods=['GET', 'POST'])
def home():
    form = LoginForm()
    if form.validate_on_submit():
        print("day" + form.email.data)
        p1 = Process(target=getPrediction, args=(form.email.data))
        p1.start()
        getPrediction(form.email.data)
        return redirect(url_for('result'))
    else:
        print("Invalid")
        # flash('Invalid Input', 'danger')
    return render_template('home.html', title='Home', form=form)


@app.route("/result")
def result():
    return render_template('result.html', title='result')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=50000)
