$(function() {
  $('form').submit(function() {
    var action = $(this).attr('action');
    var qs = $(this).serialize();
    url = action + '?' + qs
    console.log(url);
    open(url);

    return false;
  });
});
