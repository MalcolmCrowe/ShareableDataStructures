using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Microsoft.Win32;
using System.IO;
using Pyrrho;
namespace PyrrhoSQL
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public PyrrhoConnect conn = null;
        TextBox current = null;
        public MainWindow()
        {
            InitializeComponent();
        }

        void Connect()
        {
            StatusBox.Content = "";
            try
            {
                if (conn != null && conn.isOpen)
                    conn.Close();
                var s = DatabaseBox.Text;
                if (s == "")
                    s = DatabaseBox.SelectedValue.ToString();
                if (s == "")
                    return;
                conn = new PyrrhoConnect("Host=" + HostBox.Text + ";Port=" + PortBox.Text +
                    ";Files=" + s);
                conn.Open();
                RoleBox.Items.Clear();
                RoleBox.IsEnabled = true;
                LoadButton.IsEnabled = true;
                CommandBox.IsEnabled = true;
            }
            catch (DatabaseError er)
            {
                StatusBox.Content = er.Message;
            }
        }

        private void DatabaseBox_DropDownOpened(object sender, EventArgs e)
        {
            StatusBox.Content = "";
            try
            {
                if (conn != null && conn.isOpen)
                    conn.Close();
                var bconn = new PyrrhoConnect("Host=" + HostBox.Text + ";Port=" + PortBox.Text);
                bconn.Open();
                var cmd = bconn.CreateCommand();
                cmd.CommandText = "table \"Sys$Database\"";
                var rdr = cmd.ExecuteReader();
                DatabaseBox.Items.Clear();
                while (rdr.Read())
                    DatabaseBox.Items.Add(rdr.GetString(0));
                rdr.Close();
                bconn.Close();
            }
            catch (DatabaseError er)
            {
                StatusBox.Content = er.Message;
            }
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            CommandBox.Items.Clear();
            CommandBox.Items.Add(NewCommandLine());
            LoadButton.IsEnabled = false;
            InsertButton.IsEnabled = false;
            SaveButton.IsEnabled = false;
            MoveUp.IsEnabled = false;
            MoveDown.IsEnabled = false;
            DeleteButton.IsEnabled = false;
            RunSelectedButton.IsEnabled = false;
            CommandBox.IsEnabled = false;
            ClearButton.IsEnabled = false;
            RoleBox.IsEnabled = false;
        }

        TextBox NewCommandLine()
        {
            var tb = new TextBox();
            tb.TextChanged += CommandChanged;
            tb.GotFocus += CommandLineFocus;
            return tb;
        }

        void CommandChanged(object sender, TextChangedEventArgs args)
        {
            current = sender as TextBox;
            CommandBox.SelectedItem = current;
            RunSelectedButton.IsEnabled = current.Text != "";
            SaveButton.IsEnabled = true;
        }

        void CommandLineFocus(object sender, EventArgs args)
        {
            current = sender as TextBox;
            CommandBox.SelectedItem = current;
            DeleteButton.IsEnabled = true;
            InsertButton.IsEnabled = true;
            ClearButton.IsEnabled = true;
        }

        private void RoleBox_DropDownOpened(object sender, EventArgs e)
        {
            StatusBox.Content = "";
            try
            {
                RoleBox.Items.Clear();
                if (conn == null && DatabaseBox.Text!="")
                    Connect();
                if (conn == null)
                    return;
                var cmd = conn.CreateCommand();
                cmd.CommandText = "table \"Sys$Role\"";
                var rdr = cmd.ExecuteReader();
                int sel = -1;
                while (rdr.Read())
                {
                    var s = rdr.GetString(1);
                    RoleBox.Items.Add(s);
                    if (s == (string)DatabaseBox.SelectedValue)
                        sel = RoleBox.Items.Count;
                }
                rdr.Close();
                if (sel >= 0)
                    RoleBox.SelectedIndex = sel;
            }
            catch (DatabaseError er)
            {
                StatusBox.Content = er.Message;
            }

        }

        private void InsertButton_Click(object sender, RoutedEventArgs e)
        {
            if (CommandBox.SelectedIndex < 0)
                CommandBox.SelectedIndex = 0;
            CommandBox.Items.Insert(CommandBox.SelectedIndex, NewCommandLine());
        }

        private void DeleteButton_Click(object sender, RoutedEventArgs e)
        {
            if (CommandBox.SelectedItem!=null)
                CommandBox.Items.Remove(CommandBox.SelectedItem);
            if (CommandBox.Items.Count == 0)
            {
                InsertButton.IsEnabled = false;
                SaveButton.IsEnabled = false;
                MoveUp.IsEnabled = false;
                MoveDown.IsEnabled = false;
                DeleteButton.IsEnabled = false;
                RunSelectedButton.IsEnabled = false;
            }
        }

        private void CommandBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            var cb = CommandBox;
            MoveUp.IsEnabled = cb.SelectedIndex > 0;
            MoveDown.IsEnabled = cb.SelectedIndex < cb.Items.Count - 1;
        }

        private void RunSelectedButton_Click(object sender, RoutedEventArgs e)
        {
            StatusBox.Content = "";
            var rg = ResultsGrid;
            rg.Children.Clear();
            foreach(TextBox tb in CommandBox.SelectedItems)
            try
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = tb.Text;
                var rdr = cmd.ExecuteReader();
                if (rdr == null)
                    continue;
                rg.Children.Clear();
                var nc = rdr.FieldCount;
                for (var i = 0; i < nc; i++)
                {
                    var cd = new ColumnDefinition();
                    cd.Width = new GridLength(1, GridUnitType.Auto);
                    var bd = new Border();
                    bd.BorderThickness = new Thickness(0.5);
                    bd.BorderBrush = new SolidColorBrush(Colors.Cyan);
                    var lb = new TextBlock();
                    lb.Margin = new Thickness(2.0);
                    lb.Background = new SolidColorBrush(Colors.LightCyan);
                    lb.Text = rdr.GetName(i);
                    bd.Child = lb;
                    rg.ColumnDefinitions.Add(cd);
                    rg.Children.Add(bd);
                    Grid.SetRow(bd, 0);
                    Grid.SetColumn(bd, i);
                }
                var rd = new RowDefinition();
                rd.Height = new GridLength(1, GridUnitType.Auto);
                rg.RowDefinitions.Add(rd);
                int ir = 1;
                while (rdr.Read())
                {
                    rd = new RowDefinition();
                    rd.Height = new GridLength(1, GridUnitType.Auto);
                    rg.RowDefinitions.Add(rd);
                    for (int ic = 0; ic < nc; ic++)
                    {
                        var bd = new Border();
                        bd.BorderThickness = new Thickness(0.5);
                        bd.BorderBrush = new SolidColorBrush(Colors.Cyan);
                        var lb = new TextBlock();
                        lb.Margin = new Thickness(2.0);
                        var c = rdr.GetValue(ic);
                        if (c != null && c != DBNull.Value)
                            lb.Text = c.ToString();
                        bd.Child = lb;
                        rg.Children.Add(bd);
                        Grid.SetRow(bd, ir);
                        Grid.SetColumn(bd, ic);
                    }
                    ir++;
                }
                rdr.Close();
            }
            catch (DatabaseError er)
            {
                StatusBox.Content = er.Message;
            }
            if (CommandBox.SelectedIndex<CommandBox.Items.Count-1)
                CommandBox.SelectedIndex++;
        }

        private void MoveUp_Click(object sender, RoutedEventArgs e)
        {
            var x = CommandBox.SelectedItem;
            var i = CommandBox.SelectedIndex;
            CommandBox.Items.Remove(x);
            CommandBox.Items.Insert(i - 1, x);
        }

        private void MoveDown_Click(object sender, RoutedEventArgs e)
        {
            var x = CommandBox.SelectedItem;
            var i = CommandBox.SelectedIndex;
            CommandBox.Items.Remove(x);
            CommandBox.Items.Insert(i + 1, x);
        }

        private void LoadButton_Click(object sender, RoutedEventArgs e)
        {
            CommandBox.Items.Clear();
            var dlg = new OpenFileDialog();
            dlg.Filter = "SQL file|*.sql;.*txt";
            var b = dlg.ShowDialog();
            if (b == true)
            {
                StreamReader r = new StreamReader(dlg.FileName);
                var str = r.ReadLine();
                while (str!=null)
                {
                    var tb = new TextBox();
                    tb.TextChanged += CommandChanged;
                    tb.GotFocus += CommandLineFocus;
                    if (str.StartsWith("["))
                    {
                        str = str.Substring(1);
                        while (str != null)
                        {
                            if (str.EndsWith(";]"))
                            {
                                tb.Text += str.Substring(0, str.Length - 2);
                                break;
                            }
                            tb.Text += str;
                            str = r.ReadLine();
                        }
                    }
                    else
                        tb.Text = str;
                    CommandBox.Items.Add(tb);
                    str = r.ReadLine();
                }
                r.Close();
            }
            ClearButton.IsEnabled = true;
        }

        private void SaveButton_Click(object sender, RoutedEventArgs e)
        {
            var dlg = new SaveFileDialog();
            dlg.Filter = "SQL file|*.sql";
            dlg.AddExtension = true;
            var b = dlg.ShowDialog();
            if (b == true)
            {
                StreamWriter r = new StreamWriter(dlg.FileName);
                foreach (TextBox tb in CommandBox.Items)
                {
                    if (tb.Text.Contains('\n'))
                        r.WriteLine("["+tb.Text+";]");
                    else
                        r.WriteLine(tb.Text);
                }
                r.Close();
            }

        }

        private void RoleBox_DropDownClosed(object sender, EventArgs e)
        {
            StatusBox.Content = "";
            if (RoleBox.SelectedValue == null)
                return;
            try
            {
                conn.Act("set role \"" + RoleBox.SelectedValue + "\"");
            }
            catch (DatabaseError er)
            {
                StatusBox.Content = er.Message;
            }
 
        }

        private void ClearButton_Click(object sender, RoutedEventArgs e)
        {
            CommandBox.Items.Clear();
            CommandBox.Items.Add(NewCommandLine());
        }

        private void DatabaseBox_GotFocus(object sender, RoutedEventArgs e)
        {
            ConnectButton.Visibility = Visibility.Visible;
        }

        private void DatabaseBox_DropDownClosed(object sender, EventArgs e)
        {
            if (DatabaseBox.Text != "" && DatabaseBox.SelectedValue == null)
                ConnectButton.Visibility = Visibility.Visible;
            else
                ConnectButton.Visibility = Visibility.Hidden;

        }

        private void ConnectButton_Click(object sender, RoutedEventArgs e)
        {
            Connect();
        }

        private void DatabaseBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            Connect();
        }

    }
}
