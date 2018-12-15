from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Length, Email, EqualTo


class LoginForm(FlaskForm):
    email = StringField('Input(With In 14 Days, eg: 1, 2, 3)',
                        validators=[DataRequired()])
    submit = SubmitField('Submit')
